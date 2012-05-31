#!/usr/bin/env python

try:
    import json
except ImportError:
    import simplejson as json
import redis

def load(fp, host='localhost', port=6379, password=None, db=0, empty=False):
    #s = fp.read()
    r = redis.Redis(host=host, port=port, password=password, db=db)
    if empty:
        r.flushdb()   
    pipe=r.pipeline()
    size = 0
    for s in fp.xreadlines():
        table = json.loads(s)
        size = size+s.__len__()
        for key in table:
            item = table[key]
            type = item['type']
            value = item['value']
            _writer(pipe, key, type, value)
        if size>1024*1024*5:
            pipe.execute()
            pipe=r.pipeline()
            size=0
    pipe.execute()
    

def _writer(pipe, key, type, value):
    pipe.delete(key)
    if type == 'string':
        pipe.set(key, value)
    elif type == 'list':
        for element in value:
            pipe.rpush(key, element)
    elif type == 'set':
        for element in value:
            pipe.sadd(key, element)
    elif type == 'zset':
        for element in value.keys():
            pipe.zadd(key,element,value[element])
    elif type == 'hash':
        for element in value.keys():
            pipe.hset(key,element,value[element])
    else:
        raise UnknownTypeError("Unknown key type: %s" % type)


if __name__ == '__main__':
    import optparse
    import os.path
    import re
    import sys
    
    def options_to_kwargs(options):
        args = {}
        if options.host:
            args['host'] = options.host
        if options.port:
            args['port'] = int(options.port)
        if options.password:
            args['password'] = options.password
        if options.db:
            args['db'] = int(options.db)
        # dump only
        if hasattr(options, 'pretty') and options.pretty:
            args['pretty'] = True
        # load only
        if hasattr(options, 'empty') and options.empty:
            args['empty'] = True
        return args
    
    def do_load(options, args):
        if len(args) > 0:
            input = open(args[0], 'r')
        else:
            input = sys.stdin
        kwargs = options_to_kwargs(options)
        load(input, **kwargs)
        
        if len(args) > 0:
            input.close()

    
    usage = "Usage: %prog [options] [FILE]"
    usage += "\n\n Load data from FILE (which must be a JSON dump previously created"
    usage += "\n by redisdl) into specified or default redis."
    usage += "\n\n If FILE is omitted standard input is read."

    parser = optparse.OptionParser(usage=usage)
    parser.add_option('-H', '--host', help='connect to HOST (default localhost)')
    parser.add_option('-p', '--port', help='connect to PORT (default 6379)')
    parser.add_option('-s', '--socket', help='connect to SOCKET')
    parser.add_option('-w', '--password', help='connect with PASSWORD')
    parser.add_option('-d', '--db', help='load into DATABASE (0-N, default 0)')
    parser.add_option('-e', '--emptydb', help='delete all keys in destination db prior to loading')
    parser.add_option('-e', '--emptykey', help='delete the keys that we are loading')
    parser.add_option('-l', '--load', help='load data into redis (default is to dump data from redis)', action='store_true')
    options, args = parser.parse_args()    
    
    if len(args) > 1:
        parser.print_help()
        exit(4)
    do_load(options, args)
