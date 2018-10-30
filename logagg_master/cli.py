import sys
from os.path import expanduser

import ujson as json
from tabulate import tabulate
import requests
from diskdict import DiskDict
from logagg_utils import ensure_dir
from deeputil import AttrDict
from structlog.dev import ConsoleRenderer

def prGreen(txt): print("\033[92m {}\033[00m" .format(txt))
def prRed(err): print("\033[91m {}\033[00m" .format(err))

class LogaggCli():
    '''
    Command line interface for logagg
    '''
    
    MASTER_PING_URL = 'http://{host}:{port}/logagg/v1/ping?key={key}&secret={secret}'
    MASTER_ADD_NSQ_URL = 'http://{host}:{port}/logagg/v1/add_nsq?nsqd_tcp_address={nsqd_tcp_address}&nsqd_http_address={nsqd_http_address}&key={key}&secret={secret}'
    MASTER_GET_NSQ_URL = 'http://{host}:{port}/logagg/v1/get_nsq?key={key}&secret={secret}'
    CREATE_CLUSTER_URL = 'http://{host}:{port}/logagg/v1/create_cluster?cluster_name={cluster_name}'
    GET_CLUSTER_URL = 'http://{host}:{port}/logagg/v1/get_clusters'
    GET_CLUSTER_INFO_URL = 'http://{host}:{port}/logagg/v1/get_cluster_info?cluster_name={cluster_name}&cluster_passwd={cluster_passwd}'
    CHANGE_CLUSTER_PASSWD_URL = 'http://{host}:{port}/logagg/v1/change_cluster_passwd?cluster_name={cluster_name}&old_passwd={old_passwd}&new_passwd={new_passwd}'
    GET_COMPONENT_URL = 'http://{host}:{port}/logagg/v1/get_components?cluster_name={cluster_name}&cluster_passwd={cluster_passwd}'
    TAIL_LOGS_URL = 'http://{host}:{port}/logagg/v1/tail_logs?cluster_name={cluster_name}&cluster_passwd={cluster_passwd}'
    COLLECTOR_ADD_FILE_URL = 'http://{host}:{port}/logagg/v1/collector_add_file?cluster_name={cluster_name}&cluster_passwd={cluster_passwd}&collector_host={collector_host}&collector_port={collector_port}&fpath="{fpath}"&formatter="{formatter}"'
    COLLECTOR_REMOVE_FILE_URL = 'http://{host}:{port}/logagg/v1/collector_remove_file?cluster_name={cluster_name}&cluster_passwd={cluster_passwd}&collector_host={collector_host}&collector_port={collector_port}&fpath="{fpath}"'

    def __init__(self):
        self.data_path = ensure_dir(expanduser('~/.logagg'))
        self.state = DiskDict(self.data_path)
        self._init_state()


    def _init_state(self):
        '''
        Initialize default values for stored state
        '''
        if not self.state['master']:
            self.state['master'] = dict()
            self.state.flush()
        if not self.state['cluster_list']:
            self.state['cluster_list'] = list()
            self.state.flush()
        if not self.state['default_cluster']:
            self.state['default_cluster'] = dict()
            self.state.flush()


    def ensure_master(self):
        '''
        Check if Master details are present
        '''
        if not self.state['master']:
            err_msg = 'No master details, use "logagg master add" command to add one'
            prRed(err_msg)
            sys.exit(0)
        else:
            return AttrDict(self.state['master'])


    def request_master_url(self, url):
        '''
        Request mater urls and return response
        '''
        try:
            response =  requests.get(url)
            response = json.loads(response.content.decode('utf-8'))
            return response

        except requests.exceptions.ConnectionError:
            err_msg = 'Could not reach master, url: {}'.format(url)
            prRed(err_msg)


    def clear(self):
        '''
        Delete all saved data
        '''
        self.state['master'] = dict()
        self.state['cluster_list'] = list()
        self.state['default_cluster'] = dict()
        self.state.flush()


    def add_master(self, host, port, auth):
        '''
        Add master details to state file 
        '''
        ping_url = self.MASTER_PING_URL.format(host=host, port=port, key=auth.key, secret=auth.secret)
        ping_result = self.request_master_url(ping_url)

        if ping_result['result']['success']:
            if ping_result['result']['details'] == 'Authentication passed':
                master_details = {'host': host, 'port': port, 'key': auth.key, 'secret': auth.secret, 'admin': True}
                self.state['master'] = master_details
                self.state.flush()
                prGreen('Added master with admin permission')
            elif ping_result['result']['details'] == 'Authentication failed' and not auth.key and not auth.secret:
                master_details = {'host': host, 'port': port, 'key': auth.key, 'secret': auth.secret, 'admin': False}
                self.state['master'] = master_details
                self.state.flush()
                prRed('Added master with non-admin permission')
            else:
                err_msg = ping_result['result']['details']
                prRed(err_msg)


    def list_master(self):
        '''
        Show Master details
        '''
        master = self.ensure_master()
        headers = ['HOST', 'PORT', 'ADMIN']

        data = [[master.host, master.port, str(master.admin)]]
        print(tabulate(data, headers=headers))


    def add_nsq(self,  nsqd_tcp_address, nsqd_http_address):
        '''
        Add nsq details to master
        '''
        master = self.ensure_master()

        if not master.admin:
            err_msg = 'Requires admin permissions to master'
            prRed(err_msg)
            sys.exit(0)

        add_nsq_url = self.MASTER_ADD_NSQ_URL.format(host=master.host,
                                                        port=master.port,
                                                        nsqd_tcp_address=nsqd_tcp_address,
                                                        nsqd_http_address=nsqd_http_address,
                                                        key=master.key,
                                                        secret=master.secret)

        add_nsq_result = self.request_master_url(add_nsq_url)

        if add_nsq_result['result']['success']:
            prGreen(add_nsq_result['result']['details'])
        else:
            err_msg =  add_nsq_result['result']['details']
            prRed(err_msg)


    def list_nsq(self):
        '''
        List nsq details of master
        '''
        master = self.ensure_master()

        if not master.admin:
            err_msg = 'Requires admin permissions to master'
            prRed(err_msg)
            sys.exit(0)

        get_nsq_url = self.MASTER_GET_NSQ_URL.format(host=master.host,
                                                     port=master.port,
                                                     key=master.key,
                                                     secret=master.secret)

        get_nsq_result = self.request_master_url(get_nsq_url)
    
        if get_nsq_result['result']['success']:
            nsq_details = get_nsq_result['result']['nsq_list']
            headers = ['Nsqd TCP address', 'Nsqd HTTP address', 'Nsq depth limit', 'Nsq API address']
            data = list()
            for nsq in nsq_details: data.append(list(nsq.values())) 
            print(tabulate(data, headers=headers))
        else:
            err_msg = get_nsq_result['result']['details']
            prRed(err_msg)
            sys.exit(0)


    def create_cluster(self, cluster_name):
        '''
        Create a cluster in logagg-master
        '''
        master = self.ensure_master()

        create_cluster_url = self.CREATE_CLUSTER_URL.format(host=master.host,
                                                            port=master.port,
                                                            cluster_name=cluster_name)
        create_cluster_result = self.request_master_url(create_cluster_url)

        if create_cluster_result['result']['success']: 
            cluster_name = create_cluster_result['result']['cluster_name']
            cluster_password = create_cluster_result['result']['cluster_passwd']

            # Print result
            msg = 'Added cluster-name: {cluster_name} cluster-password: {cluster_password}'
            prGreen(msg.format(cluster_name=cluster_name, cluster_password=cluster_password))
        

            # Store to cluster list
            cluster_list = self.state['cluster_list']
            cluster_list.append({'cluster_name': cluster_name,
                                 'cluster_passwd': cluster_password
                            })
            self.state['cluster_list'] = cluster_list
            self.state.flush()

            # Use cluster as default cluster
            self.use_cluster(cluster_name)

        else:
            # Print result
            msg = create_cluster_result['result']['details']
            prRed(msg)


    def list_cluster(self):
        '''
        List all the clusters in master
        '''
        master = self.ensure_master()

        list_cluster_url = self.GET_CLUSTER_URL.format(host=master.host,
                                                       port=master.port)
        list_cluster_result =  self.request_master_url(list_cluster_url)

        saved_clusters = self.state['cluster_list']
        saved_cluster_names = [c['cluster_name'] for c in saved_clusters]
        cluster_list = list_cluster_result['result']

        for cluster in cluster_list:
            if cluster['cluster_name'] in saved_cluster_names: cluster['admin'] = True
            else: cluster['admin'] = False
            if cluster['cluster_name'] == self.state['default_cluster'].get('cluster_name'): cluster['default_cluster'] = True
            else: cluster['default_cluster'] = False

        headers = ['Cluster-name',
                'Nsqd TCP address',
                'NSQd TCP address',
                'NSQ max depth',
                'Nsq API address',
                'Heartbeat topic',
                'Logs topic',
                'Admin',
                'Default cluster']
        data =  list()
        for c in cluster_list: data.append(list(c.values()))
        print(tabulate(data, headers=headers))


    def join_cluster(self, cluster_name, cluster_password):
        '''
        Join an existing cluster
        '''
        master = self.ensure_master()

        get_cluster_info_url = self.GET_CLUSTER_INFO_URL.format(host=master.host,
                                                               port=master.port,
                                                               cluster_name=cluster_name,
                                                               cluster_passwd=cluster_password)

        get_cluster_info_result = self.request_master_url(get_cluster_info_url)
            
        if get_cluster_info_result['result']['success']: 
            cluster_name = get_cluster_info_result['result']['cluster_info']['cluster_name']
            cluster_password = get_cluster_info_result['result']['cluster_info']['cluster_passwd']

       
            # Store to cluster list
            cluster_info = {'cluster_name': cluster_name, 'cluster_passwd': cluster_password}
            if cluster_info not in self.state['cluster_list']:
                cluster_list = self.state['cluster_list']
                cluster_list.append(cluster_info)
                self.state['cluster_list'] = cluster_list
                self.state.flush()
                
                # Print result
                msg = 'Joined cluster-name: {cluster_name} cluster-password: {cluster_password}'
                prGreen(msg.format(cluster_name=cluster_name, cluster_password=cluster_password))
                
                # Use cluster as default cluster
                self.use_cluster(cluster_name)

            else:
                # Print result
                msg = 'Already present cluster-name: {cluster_name} cluster-password: {cluster_password}'
                prRed(msg.format(cluster_name=cluster_name, cluster_password=cluster_password))


        else:
            # Print result
            msg = get_cluster_info_result['result']['details']
            prRed(msg)


    def ensure_cluster_info(self, cluster_name):
        '''
        Ensure cluster info is saved locally
        '''
        cluster_list = self.state['cluster_list']
        
        for cluster in cluster_list:
            if cluster['cluster_name'] == cluster_name:
                return cluster
        err_msg = 'No cluster found, cluster-name: {cluster_name}'.format(cluster_name=cluster_name)
        prRed(err_msg)
        sys.exit(0)
       

    def use_cluster(self, cluster_name):
        '''
        Make a cluster usable by default
        '''
        cluster_info = self.ensure_cluster_info(cluster_name)

        self.state['default_cluster'] = cluster_info
        self.state.flush()
        prGreen('Switched to default: {}'.format(cluster_name))


    def delete_cluster(self, cluster_name):
        '''
        Delete locally saved cluster
        '''
        cluster_info = self.ensure_cluster_info(cluster_name)

        if self.state['default_cluster'] == cluster_info:
            err_msg = 'Cannot delete default cluster: {}'.format(cluster_name)
            prRed(err_msg)
        else:
            cluster_list = list()
            for c in self.state['cluster_list']:
                if c == cluster_info: pass
                else: cluster_list.append(c)

            self.state['cluster_list'] = cluster_list
            self.state.flush()
            msg = 'Deleted cluster-name: {cluster_name}'
            prGreen(msg.format(cluster_name=cluster_name))

    
    def list_collectors(self):
        '''
        List collectors in an existing cluster
        '''
        master = self.ensure_master()

        if not self.state['default_cluster']:
            err_msg = 'No default cluster'
            prRed(err_msg)
        else:
            cluster_name = self.state['default_cluster']['cluster_name']
            cluster_passwd = self.state['default_cluster']['cluster_passwd']

            get_components_url = self.GET_COMPONENT_URL.format(host=master.host,
                                                                port=master.port,
                                                                cluster_name=cluster_name,
                                                                cluster_passwd=cluster_passwd)

            get_components_result = self.request_master_url(get_components_url)

            if get_components_result['result']['success']: 
                components_info = get_components_result['result'].get('components_info')

                headers = ['Namespace',
                        'Host',
                        'Port',
                        'Cluster name',
                        'files tracked',
                        'Heartbeat number',
                        'timestamp',]

                data =  list()
                for c in components_info:
                    if c.get('namespace') == 'collector':
                        data.append([c.get('namespace'),
                                     c.get('host'),
                                     c.get('port'),
                                     c.get('cluster_name'),
                                     c.get('files_tracked'),
                                     c.get('heartbeat_number'),
                                     c.get('timestamp')]
                                     )
                print(tabulate(data, headers=headers))

            else:
                # Print result
                msg = get_components_result['result']['details']
                prRed(msg)


    def change_password_cluster(self, cluster_name, new_password, old_password):
        '''
        Change password of an existing cluster
        '''
        master = self.ensure_master()

        if not old_password:
            old_info = self.ensure_cluster_info(cluster_name)
        else:
            old_info = {'cluster_name': cluster_name, 'cluster_passwd':old_password}

        new_info = {'cluster_name': cluster_name, 'cluster_passwd':new_password}

        change_password_cluster_url = self.CHANGE_CLUSTER_PASSWD_URL.format(host=master.host,
                                                                            port=master.port,
                                                                            cluster_name=cluster_name,
                                                                            old_passwd=old_info['cluster_passwd'],
                                                                            new_passwd=new_info['cluster_passwd'])
        
        change_password_cluster_result = self.request_master_url(change_password_cluster_url)
        
        if change_password_cluster_result['result']['success']: 
            cluster_name = change_password_cluster_result['result']['cluster_info']['cluster_name']
            cluster_password = change_password_cluster_result['result']['cluster_info']['cluster_passwd']
       
            # Store to cluster list
            cluster_list = self.state['cluster_list']
            for cluster in cluster_list:
                if cluster['cluster_name'] == cluster_name: cluster['cluster_passwd'] = cluster_password
            self.state['cluster_list'] = cluster_list
            self.state.flush()
                
            # Print result
            msg = 'Changed cluster-name: {cluster_name} cluster-password: {cluster_password}'
            prGreen(msg.format(cluster_name=cluster_name, cluster_password=cluster_password))
                
            # Use cluster as default cluster
            if self.state['default_cluster']['cluster_name'] == cluster_name:
                self.use_cluster(cluster_name)

        else:
            # Print result
            msg = change_password_cluster_result['result']['details']
            prRed(msg)


    def tail(self, pretty):
        '''
        Tail the logs of a cluster
        '''
        master = self.ensure_master()

        if not self.state['default_cluster']:
            err_msg = 'No default cluster'
            prRed(err_msg)
        else:
            cluster_name = self.state['default_cluster']['cluster_name']
            cluster_passwd = self.state['default_cluster']['cluster_passwd']

            tail_logs_url = self.TAIL_LOGS_URL.format(host=master.host,
                                                        port=master.port,
                                                        cluster_name=cluster_name,
                                                        cluster_passwd=cluster_passwd)

            try:
                session = requests.session()
                resp = session.get(tail_logs_url, stream=True)
                c = ConsoleRenderer()
                for line in resp.iter_lines():
                    log = dict()
                    try:
                        result = json.loads(line.decode('utf-8'))
                        if result: log = json.loads(result)
                        else: continue
                    except ValueError:
                        print(Exception('ValueError log:{}'.format(result)))
                        continue
                    if pretty:
                        print(c(None, None, log))
                    else:
                        print(log)
            except requests.exceptions.ConnectionError:
                err_msg = 'Cannot request master'
                prRed(err_msg)
                sys.exit(0)
            except Exception as e:
                if resp: resp.close()
                raise e
                sys.exit(0)


    def collector_add_file(self, collector_host, collector_port, fpath, formatter):
        '''
        Add file to collector
        '''
        master = self.ensure_master()

        if not self.state['default_cluster']:
            err_msg = 'No default cluster'
            prRed(err_msg)
        else:
            cluster_name = self.state['default_cluster']['cluster_name']
            cluster_passwd = self.state['default_cluster']['cluster_passwd']

            add_file_url = self.COLLECTOR_ADD_FILE_URL.format(host=master.host,
                                                                port=master.port,
                                                                cluster_name=cluster_name,
                                                                cluster_passwd=cluster_passwd,
                                                                collector_host=collector_host,
                                                                collector_port=collector_port,
                                                                fpath=fpath,
                                                                formatter=formatter)

            add_file_result = self.request_master_url(add_file_url)

            if add_file_result['result']['success']: 
                new_fpaths_list = list()
                for fpath in add_file_result['result']['fpaths']: new_fpaths_list.append([fpath['fpath']])
                headers = ['File paths']
                data = list()
                #print result
                print(tabulate(new_fpaths_list, headers=headers))

            else:
                # Print result
                msg = get_components_result['result']['details']
                prRed(msg)


    def collector_remove_file(self, collector_host, collector_port, fpath):
        '''
        Remove file-path from collector
        '''
        master = self.ensure_master()

        if not self.state['default_cluster']:
            err_msg = 'No default cluster'
            prRed(err_msg)
        else:
            cluster_name = self.state['default_cluster']['cluster_name']
            cluster_passwd = self.state['default_cluster']['cluster_passwd']

            remove_file_url = self.COLLECTOR_REMOVE_FILE_URL.format(host=master.host,
                                                                port=master.port,
                                                                cluster_name=cluster_name,
                                                                cluster_passwd=cluster_passwd,
                                                                collector_host=collector_host,
                                                                collector_port=collector_port,
                                                                fpath=fpath)

            remove_file_result = self.request_master_url(remove_file_url)

            if remove_file_result['result']['success']: 
                new_fpaths_list = list()
                for fpath in remove_file_result['result']['fpaths']: new_fpaths_list.append([fpath['fpath']])
                headers = ['File paths']
                data = list()
                #print result
                print(tabulate(new_fpaths_list, headers=headers))

            else:
                # Print result
                msg = get_components_result['result']['details']
                prRed(msg)


