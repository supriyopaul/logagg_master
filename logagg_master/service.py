import uuid
from typing import Generator
import time

import ujson as json
from kwikapi import Request, BaseProtocol
import tornado
import pymongo
from pymongo import MongoClient
from deeputil import generate_random_string, keeprunning, AttrDict
import requests
from logagg_utils import log_exception, start_daemon_thread

class MasterService():
    '''
    Logagg master API
    '''
    NSQ_API_URL = 'http://{nsq_api_address}/tail?nsqd_tcp_address={nsqd_tcp_address}&topic={topic}&empty_lines={empty_lines}'
    COLLECTOR_ADD_FILE_URL = 'http://{collector_address}/collector/v1/add_file?fpath={fpath}&formatter={formatter}'
    COLLECTOR_REMOVE_FILE_URL = 'http://{collector_address}/collector/v1/remove_file?fpath="{fpath}"'
    COLLECTOR_STOP_URL = 'http://{collector_address}/collector/v1/stop'
    NSQ_DEPTH_LIMIT = 1000000

    def __init__(self, master, log):

        self.master = master
        self.log = log


    def ping(self, key:str, secret:str) -> dict:
        '''
        Sample url:
        'http://localhost:1088/logagg/v1/ping?key=xyz&secret=xxxx'
        '''
        if key == self.master.auth.key and secret == self.master.auth.secret:
            return {'success': True, 'details': 'Authentication passed'}
        else:
            return {'success': True, 'details': 'Authentication failed'}

        return {'success': False, 'details': 'Authentication failed'}


    def add_nsq(self, nsqd_tcp_address:str, nsqd_http_address:str, key:str, secret:str) -> dict:
        '''
        Insert nsq details into master
        Sample url:
        'http://localhost:1088/logagg/v1/add_nsq?nsqd_tcp_address="<hostname>:4150"&nsqd_http_address="<hostname>:4151"&key=xyz&secret=xxxx'
        '''
        if key == self.master.auth.key and secret == self.master.auth.secret:
            nsq_api = dict()
            random_nsq_api = self.master.nsq_api_collection.aggregate([{'$sample': {'size': 1}}])

            for n in random_nsq_api:
                nsq_api = n

            if not nsq_api:
                return {'success': False, 'details': 'No nsq_api in master to assign to NSQ'}

            details = {'nsqd_tcp_address': nsqd_tcp_address,
                        'nsqd_http_address': nsqd_http_address,
                        'nsq_depth_limit': self.NSQ_DEPTH_LIMIT,
                        'nsq_api_address': nsq_api['host']+':'+str(nsq_api['port'])}

            try:
                object_id = self.master.nsq_collection.insert_one(details).inserted_id

            except pymongo.errors.DuplicateKeyError as dke:
                return {'details': 'Duplicate nsq details', 'success': False}

            return {'success': True, 'details': 'Added NSQ details'}

        else:

            return {'success': False, 'details': 'Authentication failed'}


    def get_nsq(self, key:str, secret:str) -> list:
        '''
        Get nsq details
        Sample url:
        'http://localhost:1088/logagg/v1/get_nsq?key=xyz&secret=xxxx'
        '''
        if key == self.master.auth.key and secret == self.master.auth.secret:
            nsq = self.master.nsq_collection.find()
            nsq_list = list()
            for n in nsq:
                n.pop('_id')
                nsq_list.append(n)
            return {'success': True, 'nsq_list':nsq_list}
        else:
            return {'success': False, 'details': 'Authentication failed'}


    def register_nsq_api(self, key:str, secret:str, host:str, port:str) -> dict:
        '''
        Validate auth details and store details of component in master
        Sample url:
        'http://localhost:1088/logagg/v1/register_nsq_api?key=xyz&secret=xxxx&host=172.168.0.12&port=1077'
        '''

        if key == self.master.auth.key and secret == self.master.auth.secret:

            details = {'host': host,
                'port': port}

            try:
                object_id = self.master.nsq_api_collection.insert_one(details).inserted_id

            except pymongo.errors.DuplicateKeyError as dke:
                return {'details': 'Duplicate nsq_api details', 'success': True}

            return {'success': True, 'details': 'Added nsq_api details'}

        else:

            return {'success': False, 'details': 'Authentication failed'}

   
    def create_cluster(self, cluster_name:str) -> dict:
        '''
        Create cluster in master
        Sample url:
        'http://localhost:1088/logagg/v1/create_cluster?cluster_name=logagg'
        '''
        passwd = generate_random_string(8).decode('utf-8') 
        random_nsq = self.master.nsq_collection.aggregate([{'$sample': {'size': 1}}])
        nsq = dict()

        for n in random_nsq:
            nsq = n

        if not nsq:
            return {'success': False, 'details': 'No NSQ in master to assign to cluster'}

        cluster_info = {'cluster_name': cluster_name,
                'cluster_passwd': passwd,
                'nsqd_tcp_address': nsq['nsqd_tcp_address'],
                'nsqd_http_address': nsq['nsqd_http_address'],
                'nsq_depth_limit': nsq['nsq_depth_limit'],
                'nsq_api_address': nsq['nsq_api_address'],
                'heartbeat_topic': cluster_name+'_heartbeat',
                'logs_topic': cluster_name+'_logs'}
        try:
            object_id = self.master.cluster_collection.insert_one(cluster_info).inserted_id
            return {'success': True, 'cluster_name': cluster_name, 'cluster_passwd': passwd}

        except pymongo.errors.DuplicateKeyError as dke:
            return {'success': False, 'details': 'Cluster name already existing'}

    def get_clusters(self) -> list:
        '''
        Get cluster information
        Sample url:
        'http://localhost:1088/logagg/v1/get_clusters'
        '''

        clusters = self.master.cluster_collection.find()

        cluster_list = list()
        for c in clusters:
            del c['_id']
            del c['cluster_passwd']
            cluster_list.append(c)

        return cluster_list


    def get_cluster_info(self, cluster_name:str, cluster_passwd:str) -> dict:
        '''
        Get details of a particular cluster
        Sample url:
        'http://localhost:1088/logagg/v1/get_cluster_info?cluster_name=logagg&cluster_passwd=xxxx'
        '''

        cluster = self.master.cluster_collection.find_one({'cluster_name': cluster_name})
        if not cluster:
            return {'success': False, 'details': 'Cluster name not found'}
        else:
            if cluster['cluster_passwd'] == cluster_passwd:
                del cluster['_id']
                return {'success': True, 'cluster_info': cluster}
            else:
                return {'success': False, 'details': 'Authentication failed'}


    def change_cluster_passwd(self, cluster_name:str, old_passwd:str, new_passwd:str) -> dict:
        '''
        Sample url:
        'http://localhost:1088/logagg/v1/change_cluster_passwd?cluster_name=logagg&old_passwd=5cc299d1&new_passwd=qwerty'
        '''
        c = self.master.cluster_collection.find_one({'$and':[{'cluster_name': cluster_name},
                                                                {'cluster_passwd': old_passwd}
                                                                ]
                                                        })
        if not c:
            return {'success': False,
                    'details': 'Authentication failed'}
        
        else:
            query = {'$and':[{'cluster_name': cluster_name}, {'cluster_passwd': old_passwd}]}
            newvalues = { '$set': { 'cluster_passwd': new_passwd } }
            c = self.master.cluster_collection.update_one(query, newvalues)
            
            new_cluster_info = self.master.cluster_collection.find_one({'cluster_name': cluster_name})
            return{'success': True,
                    'cluster_info': {'cluster_name': new_cluster_info['cluster_name'],
                                     'cluster_passwd': new_cluster_info['cluster_passwd']
                                }
                }


    def register_component(self, namespace:str, cluster_name:str, cluster_passwd:str, host:str, port:str) -> dict:
        '''
        Validate auth details and store details of component in database
        Sample url:
        'http://localhost:1088/logagg/v1/register_component?namespace=master&cluster_name=logagg&cluster_passwd=xxxx&host=78.47.113.210&port=1088'
        '''
        c = self.master.cluster_collection.find_one({'cluster_name': cluster_name})

        if cluster_passwd == c['cluster_passwd']:
            component = {'namespace':namespace,
                    'host':host,
                    'port':str(port),
                    'cluster_name':cluster_name}

            component_info = AttrDict(component)
            try:
                cluster_name= component_info.cluster_name
                namespace = component_info.namespace
                host = component_info.host
                port = component_info.port
                self.master.component_collection.update_one({'cluster_name': cluster_name, 'namespace': namespace, 'host': host, 'port': port},
                                                     {'$set': component},
                                                     upsert=True)
                return {'success': True}

            except pymongo.errors.DuplicateKeyError as dke:
                return {'success': True, 'details': 'Duplicate component details'}
        else:
            return {'success': False, 'details': 'Authentication failed'}


    def get_components(self, cluster_name:str, cluster_passwd:str) -> dict:
        '''
        Get all components in a cluster
        Sample url:
        'http://localhost:1088/logagg/v1/get_components?cluster_name=logagg&cluster_passwd=xxxx'
        '''
        components_info = list()
        cluster =  self.master.cluster_collection.find_one({'cluster_name': cluster_name})
        if not cluster:
            return {'success': False, 'details': 'Cluster not found'}
        if cluster['cluster_passwd'] != cluster_passwd:
            return {'success': False, 'details': 'Authentication failed'}

        for c in self.master.component_collection.find({'cluster_name': cluster_name}):
            del c['_id']
            components_info.append(c)
        return {'success': True, 'components_info': components_info}


    def collector_add_file(self, cluster_name:str,
                            cluster_passwd:str,
                            collector_host:str,
                            collector_port:str,
                            fpath:str,
                            formatter:str) -> dict:
        '''
        Add files to collectors
        Sample url: 'http://localhost:1088/logagg/v1/collector_add_file?namespace=master&
                     cluster_name=logagg&cluster_passwd=xxxx&collector_host=localhost&collector_port=1088&
                     fpath="/var/log/serverstats.log"&formatter="logagg_collector.formatters.docker_file_log_driver"'
        '''
        cluster =  self.master.cluster_collection.find_one({'cluster_name': cluster_name})
        if not cluster:
            return {'success': False, 'details': 'Cluster not found'}
        if cluster['cluster_passwd'] != cluster_passwd:
            return {'success': False, 'details': 'Authentication failed'}

        collector_port = str(collector_port)
        collector = self.master.component_collection.find_one({'cluster_name': cluster_name,
                                                                'host': collector_host,
                                                                'port': collector_port,
                                                                'namespace': 'collector'})
        if not collector:
            return {'success': False, 'details': 'Collector not found'}
        else:
            collector_address = collector_host + ':' + collector_port
            add_file_url = self.COLLECTOR_ADD_FILE_URL.format(collector_address=collector_address,
                                                                fpath=fpath,
                                                                formatter=formatter)
            try:
                add_file_result = requests.get(add_file_url).content
                add_file_result = json.loads(add_file_result.decode('utf-8'))
            except requests.exceptions.ConnectionError:
                return {'success': False, 'details': 'Could not reach collector'}
            return {'success': True, 'fpaths': add_file_result['result']}


    def collector_remove_file(self, cluster_name:str,
                            cluster_passwd:str,
                            collector_host:str,
                            collector_port:str,
                            fpath:str) -> dict:
        '''
        remove file-path from collectors
        Sample url: 'http://localhost:1088/logagg/v1/collector_remove_file?namespace=master&
                     cluster_name=logagg&cluster_passwd=xxxx&collector_host=localhost&collector_port=1088&
                     fpath="/var/log/serverstats.log"'
        '''
        cluster =  self.master.cluster_collection.find_one({'cluster_name': cluster_name})
        if not cluster:
            return {'success': False, 'details': 'Cluster not found'}
        if cluster['cluster_passwd'] != cluster_passwd:
            return {'success': False, 'details': 'Authentication failed'}

        collector_port = str(collector_port)
        collector = self.master.component_collection.find_one({'cluster_name': cluster_name,
                                                                'host': collector_host,
                                                                'port': collector_port,
                                                                'namespace': 'collector'})
        if not collector:
            return {'success': False, 'details': 'Collector not found'}
        else:
            collector_address = collector_host + ':' + collector_port
            remove_file_url = self.COLLECTOR_REMOVE_FILE_URL.format(collector_address=collector_address,
                                                                fpath=fpath)
            try:
                remove_file_result = requests.get(remove_file_url).content
                remove_file_result = json.loads(remove_file_result.decode('utf-8'))
            except requests.exceptions.ConnectionError:
                return {'success': False, 'details': 'Could not reach collector'}
            return {'success': True, 'fpaths': remove_file_result['result']}


    def tail_logs(self, req:Request, cluster_name:str, cluster_passwd:str) -> Generator:
        '''
        Sample url:
        'http://localhost:1088/logagg/v1/tail_logs?cluster_name=logagg&cluster_passwd=xxxx'
        '''
        cluster =  self.master.cluster_collection.find_one({'cluster_name': cluster_name})
        if not cluster:
            return {'success': False, 'details': 'Cluster not found'}
        if cluster['cluster_passwd'] != cluster_passwd:
            return {'success': False, 'details': 'Authentication failed'}

        nsq_api_address = cluster['nsq_api_address']
        topic = cluster['logs_topic']
        nsqd_tcp_address = cluster['nsqd_tcp_address']
        url = self.NSQ_API_URL.format(nsq_api_address=nsq_api_address,
                                        nsqd_tcp_address=nsqd_tcp_address,
                                        topic=topic,
                                        empty_lines='yes')
        s = requests.session()
        resp = s.get(url, stream=True)
        start = time.time()
        log_list = list()

        for log in resp.iter_lines():
            if req._request.connection.stream.closed():
                self.log.debug('stream_closed')
                resp.close()
                self.finish()
                break
            if log:
                log_list.append(log.decode('utf-8') + '\n')
            else:
                 yield ''
            if time.time() - start >= 1:
                for l in log_list: yield l
                log_list = []
                start = time.time()

class Master():
    '''
    Logagg master class
    '''
    NSQ_API_URL = 'http://{nsq_api_address}/tail?nsqd_tcp_address={nsqd_tcp_address}&topic={topic}'
    SERVER_SELECTION_TIMEOUT = 500  # MongoDB server selection timeout
    NAMESPACE = 'master'
    UPDATE_COMPONENTS_INTERVAL = 30

    def __init__(self, host, port, mongodb, auth, log):

        self.host = host
        self.port = port
        self.auth = auth

        self.log = log
        self.mongodb = mongodb
        self.db_client = self._ensure_db_connection()
        self._init_mongo_collections()
        self.update_component_thread = start_daemon_thread(self.update_components)
        self.update_cluster_components_threads = dict()

       
    def _init_mongo_collections(self):
        # Collection for nsq details
        self.nsq_collection = self.db_client['nsq']
        self.nsq_collection.create_index([
            ('nsqd_tcp_address', pymongo.ASCENDING),
            ('nsqd_http_address', pymongo.ASCENDING)],
            unique=True)

        # Collection for nsq apis
        self.nsq_api_collection = self.db_client['nsq_api']
        self.nsq_api_collection.create_index([
            ('host', pymongo.ASCENDING),
            ('port', pymongo.ASCENDING)],
            unique=True)

        # Collection for components
        self.component_collection = self.db_client['components']
        self.component_collection.create_index([
            ('namespace', pymongo.ASCENDING),
            ('host', pymongo.ASCENDING),
            ('port', pymongo.ASCENDING),
            ('cluster_name', pymongo.ASCENDING)],
            unique=True)
        #FIXME: does not serve it's purpose of expiring records
        #self.component_collection.ensure_index('timestamp', expireAfterSeconds=60)

        # Collection for cluster info
        self.cluster_collection = self.db_client['cluster']
        self.cluster_collection.create_index([
             ('cluster_name', pymongo.ASCENDING)],
             unique=True)


    def _ensure_db_connection(self):
        url = 'mongodb://{}:{}@{}:{}'.format(self.mongodb.user,
                self.mongodb.passwd,
                self.mongodb.host,
                self.mongodb.port)

        client = MongoClient(url, serverSelectionTimeoutMS=self.SERVER_SELECTION_TIMEOUT)
        self.log.info('mongodb_server_connection_established', db=dict(self.mongodb))
        db_client = client[self.mongodb.name]

        return db_client

    @keeprunning(UPDATE_COMPONENTS_INTERVAL, on_error=log_exception)
    def _update_cluster_components(self, cluster_name):
        '''
        Starts a deamon thread for reading from heartbeat topic and updarting info in database
        '''
        cluster_info = self.cluster_collection.find_one({'cluster_name': cluster_name})
        topic = cluster_info['heartbeat_topic']
        nsqd_tcp_address = cluster_info['nsqd_tcp_address']
        nsq_api_address = cluster_info['nsq_api_address']

        url = self.NSQ_API_URL.format(nsq_api_address=nsq_api_address,
                                        nsqd_tcp_address=nsqd_tcp_address,
                                        topic=topic,
                                        empty_lines='no')
        try:
            self.log.info("updating_components", cluster=cluster_name)
            resp = requests.get(url, stream=True)
            start_read_heartbeat = time.time()
            for heartbeat in resp.iter_lines():
                heartbeat = AttrDict(json.loads(heartbeat.decode('utf-8')))
                cluster_name = heartbeat.cluster_name
                namespace = heartbeat.namespace
                host = heartbeat.host
                port = heartbeat.port
                self.component_collection.update_one({'cluster_name': cluster_name, 'namespace': namespace, 'host': host, 'port': port},
                                                     {'$set': heartbeat},
                                                     upsert=True)

        except requests.exceptions.ConnectionError:
            self.log.warn('cannot_request_nsq_api___will_try_again')

        except KeyboardInterrupt:
            if resp: resp.close()
            sys.exit(0)
        time.sleep(self.UPDATE_COMPONENTS_INTERVAL)


    @keeprunning(UPDATE_COMPONENTS_INTERVAL, on_error=log_exception)
    def update_components(self):
        '''
        Reads heartbeat and updates components
        '''
        # List of cluster names
        cluster_list = list()
        for c in self.cluster_collection.find(): cluster_list.append(c['cluster_name'])

        for cluster_name in cluster_list:
            if cluster_name not in self.update_cluster_components_threads:
                update_cluster_components_thread = start_daemon_thread(self._update_cluster_components, (cluster_name,))
                self.update_cluster_components_threads[cluster_name] = update_cluster_components_thread

        time.sleep(self.UPDATE_COMPONENTS_INTERVAL)
