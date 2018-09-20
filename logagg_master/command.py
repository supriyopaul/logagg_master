import os

from basescript import BaseScript
from deeputil import AttrDict
import tornado.ioloop
import tornado.web
from kwikapi.tornado import RequestHandler
from kwikapi import API

from service import MasterService, Master
from exceptions import InvalidArgument

class LogaggMasterCommand(BaseScript):
    DESC = 'Logagg Master service and Command line tool'

    def run(self):

        master = AttrDict()
        try:
            m = self.args.master.split(':')
            # So that order of keys is not a factor
            for a in m:
                a = a.split('=')
                if a[0] == 'host': master.host = a[-1]
                elif a[0] == 'port': master.port = a[-1]
                elif a[0] == 'key': master.key = a[-1]
                elif a[0] == 'secret': master.secret = a[-1]
                else: raise ValueError

        except ValueError:
            raise InvalidArgument(self.args.master)

        # Create LogaggService object
        ls = LogaggService(
            self.args.sqlite_db,
            master,
            self.log)

        master_api = LogaggService(ls, self.log)
        api = API()
        api.register(master_api, 'v1')

        app = tornado.web.Application([
            (r'^/logagg/.*', RequestHandler, dict(api=api)),
                ])

        app.listen(self.args.port)
        tornado.ioloop.IOLoop.current().start()

    def define_subcommands(self, subcommands):
        super(LogaggMasterCommand, self).define_subcommands(subcommands)

        master_cmd = subcommands.add_parser('runserver',
                help='Run logagg master service')

        master_cmd.set_defaults(func=self.run)
        master_cmd.add_argument(
                '--port', '-p', default=1088,
                help='port to run logagg collector service on, default: %(default)s')
        master_cmd.add_argument(
                '--master', '-m',
                help= 'Master service details, format: <host=localhost:port=1100:key=xyz:secret=xxxx>')
        master_cmd.add_argument(
                '--sqlite-db', '-d', default='logagg',
                help= 'Database name, default: %(default)s')

def main():
    LogaggMasterCommand().start()

if __name__ == '__main__':
    main()
