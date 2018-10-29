
from basescript import BaseScript
from deeputil import AttrDict
from logagg_utils import InvalidArgument

from .cli import LogaggCli

class LogaggCliCommand(BaseScript):
    DESC = 'Logagg Master service and Command line tool'

    def __init__(self):
        super().__init__()

    def _parse_auth_args(self):
        auth_dict = dict()

        if self.args.auth:
            args = self.args.auth.split(':')

            for a in args:
                key, value = a.split('=')
                auth_dict[key] = value
            
            return AttrDict(auth_dict)
        else:
            auth_dict = {'key': None, 'secret': None}
            return AttrDict(auth_dict)
            

    def add_master(self):
        try:
            auth = self._parse_auth_args()
        except:
            raise InvalidArgument(self.args.auth)

        LogaggCli().add_master(self.args.host, self.args.port, auth)

    def list_master(self):
        LogaggCli().list_master()

    def add_nsq(self):
        LogaggCli().add_nsq(self.args.nsqd_tcp_address, self.args.nsqd_http_address)

    def list_nsq(self):
        LogaggCli().list_nsq()

    def create_cluster(self):
        LogaggCli().create_cluster(self.args.cluster_name)

    def list_cluster(self):
        LogaggCli().list_cluster()

    def join_cluster(self):
        LogaggCli().join_cluster(self.args.cluster_name, self.args.cluster_password)

    def use_cluster(self):
        LogaggCli().use_cluster(self.args.cluster_name)

    def delete_cluster(self):
        LogaggCli().delete_cluster(self.args.cluster_name)

    def change_password_cluster(self):
        LogaggCli().change_password_cluster(self.args.cluster_name,
                                            self.args.new_password,
                                            self.args.old_password)

    def list_collectors(self):
        LogaggCli().list_collectors()

    def collector_add_file(self):
        LogaggCli().collector_add_file(self.args.collector_host,
                self.args.collector_port,
                self.args.fpath,
                self.args.formatter)

    def collector_remove_file(self):
        LogaggCli().collector_remove_file(self.args.collector_host,
                self.args.collector_port,
                self.args.fpath)

    def tail(self):
        LogaggCli().tail(self.args.pretty)

    def clear(self):
        LogaggCli().clear()

    def define_subcommands(self, subcommands):
        super(LogaggCliCommand, self).define_subcommands(subcommands)

        # clear
        clear_cmd = subcommands.add_parser('clear',
                help='Clear all saved data')
        clear_cmd.set_defaults(func=self.clear)

        # master
        master_cmd = subcommands.add_parser('master',
                help='Logagg-master details')
        master_cmd_subparser = master_cmd.add_subparsers()
        # master add
        master_cmd_add = master_cmd_subparser.add_parser('add',
                help='Store logagg-master details')
        master_cmd_add.set_defaults(func=self.add_master)
        master_cmd_add.add_argument(
                '--port', '-p', required=True,
                help='Port on which logagg master service is running on')
        master_cmd_add.add_argument(
                '--host', '-i', required=True,
                help='Hostname on which logagg master service is running on')
        master_cmd_add.add_argument(
                '--auth', '-a',
                help= 'Service auth details, format: <key=xyz:secret=xxxx>')
        # master list
        master_cmd_list = master_cmd_subparser.add_parser('list',
                help='Print logagg-master details')
        master_cmd_list.set_defaults(func=self.list_master)

        # master nsq
        master_cmd_nsq_subparser = master_cmd_subparser.add_parser('nsq',
                help='NSQ for logagg-master')
        # master nsq add
        master_cmd_nsq_subparser_add = master_cmd_nsq_subparser.add_subparsers()
        master_cmd_nsq_subparser_add_parser = master_cmd_nsq_subparser_add.add_parser('add',
                help='Add NSQ details for logagg-master')
        master_cmd_nsq_subparser_add_parser.set_defaults(func=self.add_nsq)
        master_cmd_nsq_subparser_add_parser.add_argument(
                '--nsqd-tcp-address', '-t', required=True,
                help='Nsqd tcp address, format: <localhost:4150>')
        master_cmd_nsq_subparser_add_parser.add_argument(
                '--nsqd-http-address', '-w', required=True,
                help='Nsqd http address, format: <localhost:4151>')
        # master nsq list
        master_cmd_nsq_subparser_list_parser = master_cmd_nsq_subparser_add.add_parser('list',
                help='Print NSQ details for logagg-master')
        master_cmd_nsq_subparser_list_parser.set_defaults(func=self.list_nsq)

        # cluster
        cluster_cmd_parser = subcommands.add_parser('cluster',
                help='Operations on clusters in master')
        cluster_cmd_subparser = cluster_cmd_parser.add_subparsers()
        # cluster create
        cluster_cmd_create = cluster_cmd_subparser.add_parser('create',
                help='Create a cluster')
        cluster_cmd_create.set_defaults(func=self.create_cluster)
        cluster_cmd_create.add_argument(
                '--cluster-name', '-n', required=True,
                help='Name of the cluster, must me unique')
        # cluster list
        cluster_cmd_list = cluster_cmd_subparser.add_parser('list',
                help='List all the clusters in master')
        cluster_cmd_list.set_defaults(func=self.list_cluster)
        # cluster join
        cluster_cmd_join = cluster_cmd_subparser.add_parser('join',
                help='Join an existing cluster')
        cluster_cmd_join.set_defaults(func=self.join_cluster)
        cluster_cmd_join.add_argument(
                '--cluster-name', '-n', required=True,
                help='Name of the cluster')
        cluster_cmd_join.add_argument(
                '--cluster-password', '-p', required=True,
                help='Password of the cluster to join')
        # cluster use
        cluster_cmd_use = cluster_cmd_subparser.add_parser('use',
                help='Use an existing cluster')
        cluster_cmd_use.set_defaults(func=self.use_cluster)
        cluster_cmd_use.add_argument(
                '--cluster-name', '-n', required=True,
                help='Name of the cluster')
        # cluster delete
        cluster_cmd_delete = cluster_cmd_subparser.add_parser('delete',
                help='Delete an existing cluster info from local list of clusters')
        cluster_cmd_delete.set_defaults(func=self.delete_cluster)
        cluster_cmd_delete.add_argument(
                '--cluster-name', '-n', required=True,
                help='Name of the cluster')
        # cluster change-password
        cluster_cmd_change_password = cluster_cmd_subparser.add_parser('change-password',
                help='Change password of an existing cluster')
        cluster_cmd_change_password.set_defaults(func=self.change_password_cluster)
        cluster_cmd_change_password.add_argument(
                '--cluster-name', '-n', required=True,
                help='Name of the cluster')
        cluster_cmd_change_password.add_argument(
                '--new-password', '-p', required=True,
                help='New password to be set')
        cluster_cmd_change_password.add_argument(
                '--old-password', '-o', default=None,
                help='Existing cluster password')
        # cluster tail
        cluster_cmd_tail = cluster_cmd_subparser.add_parser('tail',
                help='Tail logs in cluster')
        cluster_cmd_tail.set_defaults(func=self.tail)
        cluster_cmd_tail.add_argument(
                '--pretty', '-p',
                action='store_true',
                help='Print logs in pretty format')
        # cluster collector
        cluster_cmd_collector = cluster_cmd_subparser.add_parser('collector',
                help='Operations on cluster collectors')
        cluster_cmd_collector_subparser = cluster_cmd_collector.add_subparsers()
        # cluster collector list
        cluster_cmd_collector_list = cluster_cmd_collector_subparser.add_parser('list',
                 help='List all collectors')
        cluster_cmd_collector_list.set_defaults(func=self.list_collectors)
        # cluster collector add-file
        cluster_cmd_collector_add_file = cluster_cmd_collector_subparser.add_parser('add-file',
                help='Add file paths to collectors')
        cluster_cmd_collector_add_file.set_defaults(func=self.collector_add_file)
        cluster_cmd_collector_add_file.add_argument(
                '--collector-host', '-c',
                required=True,
                help='Host on which the collector is running')
        cluster_cmd_collector_add_file.add_argument(
                '--collector-port', '-p',
                required=True,
                help='Port on which collector service is running')
        cluster_cmd_collector_add_file.add_argument(
                '--fpath', '-f',
                help='File path of the log-file on the node where collector is running')
        cluster_cmd_collector_add_file.add_argument(
                '--formatter', '-b',
                help='Formatter to use for the log-file')
        # cluster collector remove-file
        cluster_cmd_collector_remove_file = cluster_cmd_collector_subparser.add_parser('remove-file',
                help='Remove file-path from collectors')
        cluster_cmd_collector_remove_file.set_defaults(func=self.collector_remove_file)
        cluster_cmd_collector_remove_file.add_argument(
                '--collector-host', '-c',
                required=True,
                help='Host on which the collector is running')
        cluster_cmd_collector_remove_file.add_argument(
                '--collector-port', '-p',
                required=True,
                help='Port on which collector service is running')
        cluster_cmd_collector_remove_file.add_argument(
                '--fpath', '-f',
                help='File path of the log-file on the node where collector is running')


def main():
    LogaggCliCommand().start()

if __name__ == '__main__':
    main()
