#!/usr/bin/env python
# atlascli.py
# Command line interface to MongoDB Atlas APIs
#
#
__version__ = "0.0.1"

import logging, argparse, sys, os, requests, urllib
import traceback, json, re
from requests.auth import HTTPDigestAuth

class App():

    def __init__(self, args, logger):
        self.args = args
        self.logger = logger
        if self.args.endpoint is None:
            logger.warn('No endpoint detected')
            self.args.endpoint = ''
        if self.args.endpoint.find('/')>1 or self.args.endpoint.find('/')==-1:
            self.args.endpoint = '/%s' % self.args.endpoint
        self.endpoint_map = {
                '/' : 'get'
                ,'/databaseUsers' : 'get'
                ,'/databaseUsers/admin/' : 'patch'
                ,'/alerts' : 'get'
                ,'/clusters' : 'get'
                ,'/clusters/.*/logs/mongodb.gz' : 'get'
        }
        self.command_map = {
                'CHANGEMONGODBUSERPASSWORD' : 'change_mongodb_user_password'
                ,'ALERTS' : 'alerts'
                ,'CLUSTERS' : 'clusters'
                ,'LOGS' : 'logs'
        }
        self.logger.debug('app.args.endpoint: %s' % self.args.endpoint)
        ep = self.args.endpoint
        # slice out query string, if any
        self.ep_no_query = ep[0:ep.find('?') if (ep.find('?')>-1) else len(ep)]
        self.logger.debug('ep_no_query: %s' % self.ep_no_query)
        # determine if command mode or just arbitrary endpoint
        self.command_mode = False
        if len(self.args.command_info)==0:
            if not self.ep_in_endpoint_map(self.ep_no_query):
                raise Exception('unsupported endpoint: %s' % self.__ep__())
        else:
            self.command_mode = True
            self.command_name = self.args.command_info[0].upper()
            if not self.command_name in self.command_map:
                raise Exception('unsuppored command: %s' % self.command_info[0])
            self.command_args = {}
            for args in self.args.command_info[1:]:
                arg_info = args.split(':')
                self.command_args[arg_info[0]]=''.join(arg_info[1:])

        # deal with data
        if self.args.data:
            if self.args.data.find('@')==1:
                with open(self.args.data[1:],'r') as file:
                    data = file.read()
            else:
                data = self.args.data
            self.data = data
        else:
            self.data = None

        if self.command_mode:
            self.data = json.dumps(self.command_args)

    def ep_in_endpoint_map(self,ep):
        result = False;
        map_ep = ''
        for e in self.endpoint_map:
            match = re.match(e,ep)
            if match:
                result = True
                map_ep = e
                break
        self.logger.debug('ep_in_endpoint_map ep=%s result=%s' % (ep,result))
        return (result, map_ep)

    def invoke(self):
        # Run a command if given, otherwise default to generic endpoint
        if self.command_mode:
            method_name = self.command_map[self.command_name]

        else:
            (junk, map_entry) = self.ep_in_endpoint_map(self.ep_no_query)
            method_name = self.endpoint_map[map_entry]

        method = getattr(self,method_name)
        return method()

    def __http_auth__(self):
        return HTTPDigestAuth(self.args.atlasuser,self.args.apikey)

    def __ep__(self,__ep=None):
        ep = 'https://cloud.mongodb.com/api/atlas/v1.0/groups/%s' % self.args.project
        ep = '%s%s' % (ep, self.args.endpoint if (__ep is None) else __ep)
        if self.args.pretty:
            ep = '%s?pretty=true' % ep
        self.logger.debug('endpoint: %s' % ep)
        return ep

    def __raise_if_missing_command_arg(self,required_args):
        missing_args = []
        for arg in required_args:
            if not arg in self.command_args:
                missing_args.append(arg)
        if len(missing_args)>0:
            raise Exception("missing required argument(s): %s" % ', '.join(missing_args))

    def alerts(self):
        self.logger.debug("alerts called")
        ep = self.__ep__('/alerts')
        return self.get(ep)

    def clusters(self):
        self.logger.debug("clusters called")
        ep = self.__ep__('/clusters')
        return self.get(ep)

    def logs(self):
        self.logger.debug("logs called")
        ep = self.__ep__('/clusters/%s/logs/mongodb.gz' % self.command_args['hostname'])
        return self.get(ep)

    def change_mongodb_user_password(self):
        self.logger.debug("change_mongodb_user_password called")
        #self.__raise_if_missing_command_arg(['username','oldpassword','newpassword'])
        self.__raise_if_missing_command_arg( ('username','password') )
        ep = self.__ep__('/databaseUsers/admin/%s' % self.command_args['username'])
        return self.patch(ep)

    def get(self,url=None):
        if url is None:
            url = self.__ep__()
        self.logger.debug('get() GET: %s' % url)
        response= requests.get(url,auth=self.__http_auth__())
        try:
            response.raise_for_status()
        except Exception as exp:
            self.logger.error(json.dumps(response.json()))
            raise exp
        self.logger.debug('get() response: %s' % response)
        return response

    def post(self,url=None):
        raise Exception("NOT IMPLEMENTED")

    def patch(self,url=None,data=None):
        if url is None:
            url = self.__ep__()
        if data is None:
            data = self.data
        headers = { "Content-Type" : "application/json" }
        response = requests.patch(url,
                    auth=self.__http_auth__()
                    ,data=data
                    ,headers=headers)
        self.logger.debug('patch() response: %s' % response)
        return response

    def delete(self):
        raise Exception("NOT IMPLEMENTED")

def main():
    # parse arguments
    description = u'atlascli - Command Line Interface to MongoDB Atlas API'
    epilog = ('''atlascli can call an arbitray MongoDB Atlas API endpoint by specifing the url or supports a command mode syntax. The supported commands are:
    - changeMongoDBUserPassword username:<username> password:<password>
    - alerts
    - clusters
    - logsForHost host:<host>
Consult https://docs.atlas.mongodb.com/api/ for full details on command arguments.''')
    parser = argparse.ArgumentParser(description=description,epilog=epilog
                                     ,formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--version",action='store_true',default=False,help='print version and exit')
    parser.add_argument("--pretty",action='store_true',default=False,help='pretty format JSON')
    parser.add_argument("--loglevel",default='error'
                         ,help='loglevel debug,info default=info')
    parser.add_argument("--logfile",default='--',help='logfile full path or -- for stdout')
    parser.add_argument("--atlasuser",help='Atlas user which has access to Atlas API')
    parser.add_argument("--apikey",help='API key for Atlas user')
    parser.add_argument('--project',help='Atlas Project Id')
    parser.add_argument("command_info",nargs="*",help='format: command key1:value1 key2:value2 key3:value3 '
                        + '... \na supported command followed by command inputs'
                        + '')
    parser.add_argument('--endpoint',help='Atlas API Endpoint')
    parser.add_argument('--data',help='Data to send, use @<filename> to read data from file.')

    args = parser.parse_args()
    if (args.version):
	print('atlascli version: %s' % __version__ )
        os._exit(0)
    logger = logging.getLogger("atlascli")
    logger.setLevel(getattr(logging,args.loglevel.upper()))
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(threadName)s - %(message)s")
    if args.logfile == '--':
        handler = logging.StreamHandler(sys.stdout)
    else:
        handler = logging.FileHandler(os.path.abspath(args.logfile))
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.info(description)
    logger.info('version: %s' % __version__)
    logger.debug("args: " + str(args))
    logger.info("log level set to " + logging.getLevelName(logger.getEffectiveLevel()))
    required_args = [ 'atlasuser', 'apikey', 'project' ]
    missing_args = []
    for arg in required_args:
        if getattr(args,arg) is None:
            missing_args.append(arg)
    if len(missing_args)>0:
        logger.error('error: missing required argument(s): %s' % ', '.join(missing_args))
        os._exit(1)

    app = App(args, logger)
    logger.info('atlascli initialized')
    try:
        logger.info('running...')
        result = app.invoke()
        if args.pretty:
            print(json.dumps(result.json(), indent=2))
        else:
            print(json.dumps(result.json()))
        logger.info('atlascli done')
        os._exit(0)
    except Exception as exp:
        logger.error(exp)
        logger.debug("got exception going to call sys.exit(1)")
        traceback.print_exc()
        os._exit(1)


if __name__ == '__main__':
    main()

