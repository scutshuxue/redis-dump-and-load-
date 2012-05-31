#!/usr/bin/env python
try:
    import json
except ImportError:
    import simplejson as json
import redis

def dump(fp, host='localhost', port=6379, password=None, db=0, pretty=False):
    r = redis.Redis(host=host, port=port, password=password, db=db)
    kwargs = {}
    if not pretty:
        kwargs['separators'] = (',', ':')
    else:
        kwargs['indent'] = 2
        kwargs['sort_keys'] = True
    encoder = json.JSONEncoder(**kwargs)
    #fp.write('{')
    #first = True
    for key, type, value in _reader(r, pretty):
        d = {}
        d[key]={'type':type,'value':value}
        item = encoder.encode(d) #.strip('{').strip('}')
        fp.write(item)
        fp.write("\n")
    #fp.write('}')

def _reader(r, pretty):
    for key in r.keys():
        type = r.type(key)
        if type == 'string':
            value = r.get(key)
        elif type == 'list':
            value = r.lrange(key, 0, -1)
        elif type == 'set':
            value = list(r.smembers(key))
            if pretty:
                value.sort()
        elif type == 'zset':
            value = r.zrange(key, 0, -1, False, True)
        elif type == 'hash':
            value = r.hgetall(key)
        else:
            raise UnknownTypeError("Unknown key type: %s" % type)
        yield key, type, value


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
    
    def do_dump(options):
        if options.output:
            output = open(options.output, 'w')
        else:
            output = sys.stdout
        
        kwargs = options_to_kwargs(options)
        dump(output, **kwargs)
        
        if options.output:
            output.close()


    usage = "Usage: %prog [options]"
    usage += "\n\nDump data from specified or default redis."
    usage += "\n\nIf no output file is specified, dump to standard output."

    parser = optparse.OptionParser(usage=usage)
    parser.add_option('-H', '--host', help='connect to HOST (default localhost)')
    parser.add_option('-p', '--port', help='connect to PORT (default 6379)')
    #parser.add_option('-s', '--socket', help='connect to SOCKET')
    parser.add_option('-w', '--password', help='connect with PASSWORD')
    parser.add_option('-d', '--db', help='dump DATABASE (0-N, default 0)')
    parser.add_option('-o', '--output', help='write to OUTPUT instead of stdout')
    #parser.add_option('-y', '--pretty', help='Split output on multiple lines and indent it', action='store_true')
    
    options, args = parser.parse_args()
    #print args
    #print options
    if len(args) > 0:
        parser.print_help()
        exit(4)
    do_dump(options)

