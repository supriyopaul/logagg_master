import uuid
from typing import Generator
import time

import ujson as json
from kwikapi import Request
import tornado
import pymongo
from pymongo import MongoClient
from deeputil import generate_random_string, keeprunning
import requests
from logagg_utils import log_exception, start_daemon_thread

class MasterService():
    '''
    Logagg master API
    '''
    NSQ_API_URL = 'http://{nsq_api_address}/tail?nsqd_tcp_address={nsqd_tcp_address}&topic={topic}'
    NSQ_DEPTH_LIMIT = 1000000

    def __init__(self, master, log):

        self.master = master
        self.log = log

    def set_nsq(self, nsqd_tcp_address:str, nsqd_http_address:str, cluster_name:str) -> list:
        '''
        Insert nsq details into master
        Sample url:
        'http://localhost:1088/logagg/v1/set_nsq?nsqd_tcp_address="<hostname>:4150"&nsqd_http_address="<hostname>:4151"&cluster_name=logagg'
        '''
        details = {'nsqd_tcp_address': nsqd_tcp_address,
                    'nsqd_http_address': nsqd_http_address,
                    'cluster_name': cluster_name,
                    'log_topic': cluster_name+'_logs',
                    'heartbeat_topic': cluster_name+'_heartbeat#ephemeral',
                    'nsq_depth_limit': self.NSQ_DEPTH_LIMIT}

        try:
            object_id = self.master.nsq_collection.insert_one(details).inserted_id

        except pymongo.errors.DuplicateKeyError as dke:
            self.log.info('duplicate_details', err=dke.details)
            
            return {'duplicate_details': dke.details}

        return {'object_id': object_id.__str__()}

    def get_nsq(self, cluster_name:str) -> dict:
        '''
        Get nsq details
        Sample url:
        'http://localhost:1088/logagg/v1/get_nsq?cluster_name=logagg'
        '''
        nsq = self.master.nsq_collection.find_one()
        nsq.pop('_id')
        
        return nsq

    def register_component(self, namespace:str, cluster_name:str, cluster_passwd:str, host:str, port:str) -> dict:
        '''
        Validate auth details and store details of component in database
        Sample url:
        'http://localhost:1088/logagg/v1/register_component?namespace=master&cluster_name=logagg&cluster_passwd=xxxx&host=78.47.113.210&port=1088'
        '''
        c = self.master.cluster_collection.find_one({'cluster_name': cluster_name})

        if cluster_passwd == c['cluster_passwd']:
            component_info = {'namespace':namespace,
                    'host':host,
                    'port':port,
                    'cluster_name':cluster_name}
            try:
                object_id = self.master.component_collection.insert_one(component_info).inserted_id
                return {'authentication': 'passed'}

            except pymongo.errors.DuplicateKeyError as dke:
                self.log.error('duplicate_details', err=dke.details)

                return {'authentication': 'passed', 'duplicate_details': dke.details}
        else:
            return {'authentication': 'failed'}

    def tail_heartbeat(self, req:Request, cluster_name:str) -> Generator:
        '''
        Sample url:
        'http://localhost:1077/nsq_api/tail?nsqd_tcp_address=195.201.98.142:4150&topic=Heartbeat'
        '''
        nsq_api = self.master.component_collection.find_one({'$and':[{'cluster_name': cluster_name}, {'namespace':'nsq_api'}]})
        nsq = self.master.nsq_collection.find_one({'cluster_name': cluster_name})
        topic = nsq['heartbeat_topic']
        host = nsq_api['host']
        port = str(nsq_api['port'])
        nsqd_tcp_address = nsq['nsqd_tcp_address']

        url = self.NSQ_API_URL.format(nsq_api_address=host+':'+port,nsqd_tcp_address=nsqd_tcp_address,topic=topic)
        s = requests.session()
        resp = s.get(url, stream=True)
        for chunk in resp.iter_content(chunk_size=1024):
            if req._request.connection.stream.closed():
                self.log.info('stream_closed')
                resp.close()
                self.finish()
                break
            else:
                self.log.info('yeilding')
                yield chunk.decode('utf-8')


    def create_cluster(self, cluster_name:str) -> dict:
        '''
        Create cluster in master
        Sample url:
        'http://localhost:1088/logagg/v1/create_cluster?cluster_name=logagg'
        '''
        passwd = generate_random_string(8).decode('utf-8') 

        cluster_info = {'cluster_name': cluster_name,
                'cluster_passwd': passwd}
        try:
            object_id = self.master.cluster_collection.insert_one(cluster_info).inserted_id
            return {'cluster_name': cluster_name, 'cluster_passwd': passwd}

        except pymongo.errors.DuplicateKeyError as dke:
            return {'duplicate_details': dke.details}


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

            return {'authentication': 'failed'}
        
        else:
            query = {'$and':[{'cluster_name': cluster_name}, {'cluster_passwd': old_passwd}]}
            newvalues = { '$set': { 'cluster_passwd': new_passwd } }
            c = self.master.cluster_collection.update_one(query, newvalues)
            
            new_cluster_info = self.master.cluster_collection.find_one({'cluster_name': cluster_name})
            return{'authentication': 'passed',
                    'cluster_name': new_cluster_info['cluster_name'],
                    'passwd': new_cluster_info['cluster_passwd']
                    }

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
        
        # Collection for nsq details
        self.nsq_collection = self.db_client['nsq']
        self.nsq_collection.create_index([
            ('nsqd_tcp_address', pymongo.ASCENDING),
            ('nsqd_http_address', pymongo.ASCENDING)],
            unique=True)

        # Collection for components
        self.component_collection = self.db_client['components']
        self.component_collection.create_index([
            ('namespace', pymongo.ASCENDING),
            ('host', pymongo.ASCENDING),
            ('port', pymongo.ASCENDING)],
            unique=True)
        #FIXME: does not serve it's purpose of expiring records
        self.component_collection.ensure_index('timestamp', expireAfterSeconds=60)

        # Collection for cluster info
        self.cluster_collection = self.db_client['cluster']
        self.cluster_collection.create_index([
             ('cluster_name', pymongo.ASCENDING)],
             unique=True)

        update_component_th = start_daemon_thread(self.update_component)


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
    def update_component(self):
        '''
        Reads heartbeat and updates components
        '''

        cluster_list = list()
        nsq_apis = list()

        for nsq in self.nsq_collection.find():
            cluster_list.append(nsq['cluster_name'])

        for cluster_name in cluster_list:
            nsq_apis.append(self.component_collection.find_one({'$and':[{'cluster_name': cluster_name}, {'namespace':'nsq_api'}]}))

        for cluster_name in cluster_list:
            topic = None
            nsqd_tcp_address = None
            host = None
            port = None

            for nsq in self.nsq_collection.find():
                if nsq['cluster_name'] == cluster_name:
                    topic = nsq['heartbeat_topic']
                    nsqd_tcp_address = nsq['nsqd_tcp_address']

            for nsq_api in nsq_apis:
                try:
                    if nsq_api['cluster_name'] == cluster_name:
                        host = nsq_api['host']
                        port = str(nsq_api['port'])
                except TypeError:
                    self.log.error('component_not_found', cluster=cluster_name, component='nsq_api')
            
            if topic and nsqd_tcp_address and host and port:
                url = self.NSQ_API_URL.format(nsq_api_address=host+':'+port,nsqd_tcp_address=nsqd_tcp_address,topic=topic)
                s = requests.session()
                try:
                    resp = requests.get(url, stream=True)
                    start_read_heartbeat = time.time()

                    for heartbeat in resp.iter_lines():
                        if not time.time() - start_read_heartbeat > self.UPDATE_COMPONENTS_INTERVAL:
                            heartbeat = json.loads(heartbeat.decode('utf-8'))
                            self.component_collection.update_one({}, {'$set': heartbeat}, upsert=True)
                        else:
                            resp.close()
                            break

                except requests.exceptions.ConnectionError:
                    self.log.warn('cannot_request_nsq_api___will_try_again')

            self.log.debug("updated_components", cluster=cluster_name)
        time.sleep(self.UPDATE_COMPONENTS_INTERVAL)
