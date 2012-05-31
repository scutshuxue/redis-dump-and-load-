#!/usr/bin/env python
try:
    import json
except ImportError:
    import simplejson as json
import redis,threading,time
from Queue import Queue


class loadThread(threading.Thread):
    def __init__(self, threadname,r,queue,emptykey=False):
        threading.Thread.__init__(self, name=threadname)
        self.redis = r
        self.queue = queue
        self.emptyKey=emptykey

    def run(self):
        r = self.redis
        pipe=r.pipeline()
        size=0
        while True:
            total_line = self.queue.get()
            if total_line == "##end##":
                break
            lines = total_line.split('\n')
            for s in lines:
                #print 1                
                table = json.loads(s)
                size = size+s.__len__()
                for key in table:
                    item = table[key]
                    type = item['type']
                    value = item['value']
                    self._writer(pipe, key, type, value,self.emptyKey)
                if size>1024*1024*3:
                    pipe.execute()
                    pipe=r.pipeline()
                    size=0
        pipe.execute()
        
    def _writer(self,pipe, key, type, value,empty=False):
        if empty:
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
            for element,v in value:
                pipe.zadd(key,element,value)
        elif type == 'hash':
            for element in value.keys():
                pipe.hset(key,element,value[element])
        else:
            raise UnknownTypeError("Unknown key type: %s" % type)
    

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
        return args
    
    def do_load(options, args):
        parallel=int(options.parallel)
        
        if len(args) > 0:
            input = open(args[0])
        else:
            input = sys.stdin
        kwargs = options_to_kwargs(options)
        pool = getRedisPool(**kwargs)
        r = redis.Redis(connection_pool=pool)
        threadList=[]
        q = Queue(parallel*2)
        emptykey=False
        if options.emptykey:
            emptykey=True
        for i in range(0,parallel):            
            d = loadThread('load_' + str(i),r,q,emptykey)            
            d.setDaemon(True)
            d.start()
            threadList.append(d)
        #load(input, **kwargs)
        if options.emptydb:
            r.flushdb()
        #print dir(r)
        leftStr=''
        while True:
            total_line = leftStr + input.read(24*1024*1024)
            if total_line==None or total_line=='':
                break
            index = total_line.rfind('\n')
            if index!=-1:
                q.put(total_line[0:index])
                leftStr = total_line[index+1:]
            else:
                leftStr = leftStr+total_line
            
#        for s in input.xreadlines():
#            q.put(s)
            
        for t in threadList:            
            q.put('##end##')
        for t in threadList:
            while t.isAlive():
                time.sleep(0.5);
                
        if len(args) > 0:
            input.close()

    
    usage = "Usage: %prog [options] [FILE]"
    usage += "\n\n Load data from FILE (which must be a JSON dump previously created"
    usage += "\n by redisdl) into specified or default redis."
    usage += "\n\n If FILE is omitted standard input is read."

    parser = optparse.OptionParser(usage=usage)
    parser.add_option('-H', '--host', help='connect to HOST (default localhost)')
    parser.add_option('-p', '--port', help='connect to PORT (default 6379)')
    parser.add_option('-w', '--password', help='connect with PASSWORD')
    parser.add_option('-d', '--db',default=0, help='load into DATABASE (0-N, default 0)')
    parser.add_option('-e', '--emptydb', help='delete all keys in destination db prior to loading', action='store_true')
    parser.add_option('-c', '--emptykey', help='delete the keys that we are loading', action='store_true')
    #parser.add_option('-l', '--load', help='load data into redis (default is to dump data from redis)', action='store_true')
    parser.add_option('--parallel',default=16, help='number of parallel')
    options, args = parser.parse_args()    
    if len(args) > 1:
        parser.print_help()
        exit(4)
    do_load(options, args)
