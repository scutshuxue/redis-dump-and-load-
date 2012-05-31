#!/usr/bin/env python
try:
    import json
except ImportError:
    import simplejson as json
import redis,threading,time
from Queue import Queue

class dumpThread(threading.Thread):
    def __init__(self, threadname,keys, r,outqueue):
        threading.Thread.__init__(self, name=threadname)
        self.redis = r
        self.outqueue = outqueue
        self.keys = keys
    def run(self):
        kwargs = {}
        kwargs['separators'] = (',', ':')
        encoder = json.JSONEncoder(**kwargs)
        for key, type, value in self._reader(self.redis,self.keys):
            d = {}
            d[key]={'type':type,'value':value}
            item = encoder.encode(d) #.strip('{').strip('}')
            self.outqueue.put(item)
        #print 'end'
        
    def _reader(self,r,keys):
        for key in keys:
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
            
class writeThread(threading.Thread):
    def __init__(self, threadname,queue, fd):
        threading.Thread.__init__(self, name=threadname)
        self.queue = queue
        self.fd = fd
    def run(self):
        while True:
            line = self.queue.get()
            if line=='##end##':
                break
            else:
                self.fd.write(line + "\n")
        #print 'writeThead end'

def getRedisPool( host='localhost', port=6379, password=None, db=0):
    try:
        pool = redis.ConnectionPool(host=host, port=port,password=password,db=db,max_connections=50)  
    except Exception as e:
        print_log('CRITICAL %s' % (e))
        sys.exit(2)
    return pool

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
        threadList=[]
        parallel=int(options.parallel)
        if options.output:
            output = open(options.output, 'w')
        else:
            output = sys.stdout
        q = Queue(2000)
        kwargs = options_to_kwargs(options)
        pool = getRedisPool(**kwargs)
        r = redis.Redis(connection_pool=pool)
        w = writeThread('writeThread',q,output)
        w.setDaemon(True)
        w.start()
        #threadList.append(w)
        keys = r.keys()
        nums = keys.__len__()/parallel+1
        for i in range(0,parallel):
            #print i*nums,(i+1)*nums-1
            d = dumpThread('dump_' + str(i),keys[i*nums:(i+1)*nums], r,q)
            
            d.setDaemon(True)
            d.start()
            threadList.append(d)
        #dump(output, **kwargs)
        #print 'thread start'
        #print dir(w)
        
        for t in threadList:
            while t.isAlive():
                time.sleep(0.5);
        
            
        #print 'thread end'
        q.put("##end##")
        while w.isAlive():
            time.sleep(0.5)
        #w.join()
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
    parser.add_option('--parallel',default=16, help='number of parallel')
    #parser.add_option('-y', '--pretty', help='Split output on multiple lines and indent it', action='store_true')
    
    options, args = parser.parse_args()
    #print args
    #print options
    if len(args) > 0:
        parser.print_help()
        exit(4)
    do_dump(options)

