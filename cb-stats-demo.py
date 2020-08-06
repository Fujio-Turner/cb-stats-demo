#!/usr/bin/python
import json
import urllib2
import time
import datetime
import base64
import os
import cProfile
import pstats
import io
import thread

# import sys

'''
def profile(fnc):
    
    """A decorator that uses cProfile to profile a function"""
    
    def inner(*args, **kwargs):
        
        pr = cProfile.Profile()
        pr.enable()
        retval = fnc(*args, **kwargs)
        pr.disable()
        s = io.StringIO()
        sortby = 'cumulative'
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        print(s.getvalue())
        return retval

    return inner


def profile(func):
    def profiled_func(*args, **kwargs):
        profile = cProfile.Profile()
        try:
            profile.enable()
            result = func(*args, **kwargs)
            profile.disable()
            return result
        finally:
            profile.print_stats()
    return profiled_func
'''

class CBSTATSPULLER():
    hostname = '127.0.0.1'
    port = '8091'
    debug = False
    username = "read-only"
    password = "password"
    logPath = "/tmp/logs/"
    defaultDtFormat = "%Y-%m-%d %H:%M:%S"
    universalTimeFromCBSys = None
    oneTimeLogSys = ["cpu_idle_ms","cpu_local_ms","cpu_utilization_rate","curr_connections","mem_used_sys","mem_total","mem_free","mem_actual_free","mem_actual_used","swap_used","swap_total","hibernated_requests","hibernated_waked","rest_requests"]
    oneTimeLogIndex = ["index_ram_percent","index_memory_used","index_memory_quota","index_remaining_ram"]
    oneTimeLogQuery = ["query_warnings","query_request_time","query_result_count","query_selects","query_requests_500ms","query_active_requests","query_requests_5000ms","query_requests_1000ms","query_invalid_requests","query_queued_requests","query_avg_svc_time","query_errors","query_requests","query_avg_response_size","query_result_size","query_avg_req_time","query_requests_250ms"]
    oneTimeLogFts = ["fts_total_queries_rejected_by_herder","fts_curr_batches_blocked_by_herder","fts_num_bytes_used_ram"]
    oneTimeLogAnalytics = ["cbas_disk_used","cbas_gc_time","cbas_thread_count","cbas_gc_count","cbas_heap_used","cbas_system_load_average"]
    logElements = {"query":True,"index":True,"sysCluster":True,"sys":True,"kv":True,"xdcr":True,"fts":True,"eventing":True,"analytics":True,"analytics4Bucket":True}

    def __init__(self, config):
        self.hostname = config["hostname"]
        self.port = str(config["port"])
        self.username = config["username"]
        self.password = config["password"]
        self.logPath = config["path"]
        self.defaultDtFormat = config["dtFormat"]
        if config["secure"] == True:
            self.secure = "https"
        else:
            self.secure = "http"
        if config["debug"] == True:
            self.debug = True
        else:
            self.debug = False

        '''--------Common Methods BEGIN---------'''

    def httpGet(self, url='', retry=0):

        try:
            base64string = base64.encodestring('%s:%s' % (self.username, self.password)).replace('\n', '')
            request = urllib2.Request(url)
            request.add_header("Authorization", "Basic %s" % base64string)
            result = urllib2.urlopen(request)
            data = result.read()
            r = self.jsonChecker(data)
            return r
        except Exception, e:
            if e:
                if hasattr(e, 'code'):
                    print "Error: HTTP GET: " + str(e.code)
            if retry == 3:
                if self.debug == True:
                    print "DEBUG: Tried 3 times could not execute: GET"
                if e:
                    if hasattr(e, 'code'):
                        if self.debug == True:
                            print "DEBUG: HTTP CODE ON: GET - " + str(e.code)
                        return e.code
                    else:
                        return False
            time.sleep(1.0)
            return self.httpGet(url, retry + 1)

    def unixToDt(self, unix=''):
        return datetime.datetime.fromtimestamp(int(unix)).strftime(self.defaultDtFormat)

    def jsonChecker(self, data=''):
        # checks if its good json and if so return back Python Dictionary
        try:
            return json.loads(data)
        except Exception, e:
            return False

    def sayHelloTest(self):
        print "hello"

        '''--------Common Methods END---------'''

    def pullCbBuckets(self):
        url = self.secure + "://" + self.hostname + ":" + self.port + "/pools/default/buckets/"
        if self.debug == True:
            print("DEBUG: ", url)

        data = self.httpGet(url)

        if data == False or None:
            now = time.strftime(self.defaultDtFormat)
            data = now + " error=could not get stats \n"
            self.writeLog(data)
            return False

        if self.debug == True:
            print("DEBUG: ", data)
        return data

    def bucketsList(self):
        bucketsList = self.pullCbBuckets()

        if bucketsList == False:
            exit()
        bucket = []
        nodes = []
        data = {}

        for x in bucketsList:
            bucket.append(x["name"])
            '''
			for y in x["nodes"]:
				nodes.append(y["hostname"])
			'''
        data["bucketList"] = bucket
        # data["nodeList"] = nodes

        if self.debug == True:
            print "DEBUG: Bucket List " + json.dumps(data)
        return data

    def pullCbStatus(self):
        cbList = self.bucketsList()
        dataFull = []
        ''' http://127.0.0.1:8091/_uistats?bucket=todo&haveTStamp={"todo":1499910229945,"@system":1499910229945,"@fts":1499910229945,"@fts-todo":1499910229945,"@index":1499910229945,"@index-todo":1499910229945,"@query":1499910229945,"@xdcr-todo":0}&zoom=minute' '''
        for x in cbList["bucketList"]:
            url = self.secure + "://" + self.hostname + ":" + self.port + "/_uistats?bucket=" + x + "&zoom=minute"
            if self.debug == True:
                print("DEBUG: ", url)
            data = self.httpGet(url)
            if self.debug == True:
                print("DEBUG: ", json.dumps(data))
            data["bucket"] = x
            dataFull.append(data)
        return dataFull

    #@profile
    def makeLog(self):
        data = self.pullCbStatus()
        fullLogString = ""
        timeLoop1curr = 0
        timeLoop1 = 0
        for x in data:
            bucketName = str(x["bucket"])
            timeStamp = x["stats"]["@system"]["timestamp"]
            self.universalTimeFromCBSys = timeStamp
            unixTodt_trim = []
            for y in timeStamp:
                unixTodt_trim.append(self.unixToDt(y / 1000))  # I have to trim the time and convert to human readable DT

            if self.debug == True:
                print("DEBUG: unix to DT conversion ", unixTodt_trim)

            x["stats"]["@system"].pop("timestamp")  # removes the field timestamp 

            # kV FOR CLUSTER
            if self.logElements["kv"] == True:
                kvBucketName = "@kv-" + bucketName
                if kvBucketName in x["stats"]:
                    x["stats"][kvBucketName].pop("timestamp")
                    for key1, value1 in x["stats"][kvBucketName].items():
                        timeLoop2 = 0
                        for a in value1:
                            whatBucket = x["bucket"]
                            if key1 in self.oneTimeLogSys and timeLoop1 == 0:
                                whatBucket = 'sys'  # only do system level logging once
                            elif key1 in self.oneTimeLogSys and timeLoop1 > 0:
                                continue
                            log_string = str(unixTodt_trim[timeLoop2]) + " cb=" + whatBucket + " " + str(key1) + "=" + str(a) + '\n'
                            timeLoop2 = timeLoop2 + 1
                            fullLogString = fullLogString + log_string
                        else:
                            timeLoop2 = 0

            # System FOR CLUSTER
            if self.logElements["sys"] == True:
                if "@system" in x["stats"]:
                    x["stats"]["@system"].pop("timestamp")
                    for key2, value2 in x["stats"]["@system"].items():
                        timeLoop3 = 0
                        for b in value2:
                            whatBucket = 'sys'
                            if key2 in self.oneTimeLogSys and timeLoop1 == 0:
                                whatBucket = 'sys'  # only do system level logging once
                                log_string = str(unixTodt_trim[timeLoop3]) + " cb=sys " + str(key2) + "=" + str(b) + '\n'
                                timeLoop3 = timeLoop3 + 1
                                fullLogString = fullLogString + log_string
                            elif key2 in self.oneTimeLogSys and timeLoop1 > 0:
                                continue
                        else:
                            timeLoop3 = 0

            # QUERY FOR CLUSTER
            if self.logElements["query"] == True:
                if "@query" in x["stats"]:
                    x["stats"]["@query"].pop("timestamp")
                    for key3, value2 in x["stats"]["@query"].items():
                        timeLoop4 = 0
                        for b in value2:
                            whatBucket = x["bucket"]
                            if key3 in self.oneTimeLogQuery and timeLoop1 == 0:
                                if key3 in self.oneTimeLogSys and timeLoop1 == 0:
                                    whatBucket = 'sys'  # only do system level logging once
                                elif key3 in self.oneTimeLogSys and timeLoop1 > 0:
                                    continue
                                log_string = str(unixTodt_trim[timeLoop4]) + " cb=sys " + str(key3) + "=" + str(b) + '\n'
                                timeLoop4 = timeLoop4 + 1
                                fullLogString = fullLogString + log_string
                            elif key3 in self.oneTimeLogQuery and timeLoop1 > 0:
                                continue
                        else:
                            timeLoop4 = 0

            # FTS FOR CLUSTER
            if self.logElements["fts"] == True:
                if "@fts" in x["stats"]:
                    x["stats"]["@fts"].pop("timestamp")
                    for key4, value3 in x["stats"]["@fts"].items():
                        timeLoop5 = 0
                        for c in value3:
                            whatBucket = x["bucket"]
                            if key4 in self.oneTimeLogFts and timeLoop1 == 0:
                                if key4 in self.oneTimeLogSys and timeLoop1 == 0:
                                    whatBucket = 'sys'  # only do system level logging once
                                elif key4 in self.oneTimeLogSys and timeLoop1 > 0:
                                    continue
                                log_string = str(unixTodt_trim[timeLoop5]) + " cb=sys " + str(key4) + "=" + str(c) + '\n'
                                timeLoop5 = timeLoop5 + 1
                                fullLogString = fullLogString + log_string
                            elif key4 in self.oneTimeLogFts and timeLoop1 > 0:
                                continue
                        else:
                            timeLoop5 = 0
            # INDEX FOR CLUSTER
            if self.logElements["index"] == True:
                if "@index" in x["stats"]:
                    x["stats"]["@index"].pop("timestamp")
                    for key5, value4 in x["stats"]["@index"].items():
                        timeLoop6 = 0
                        for d in value4:
                            whatBucket = x["bucket"]
                            if key5 in self.oneTimeLogIndex and timeLoop1 == 0:
                                if key5 in self.oneTimeLogSys and timeLoop1 == 0:
                                    whatBucket = 'sys'  # only do system level logging once
                                elif key5 in self.oneTimeLogSys and timeLoop1 > 0:
                                    continue
                                log_string = str(unixTodt_trim[timeLoop6]) + " cb=sys " + str(key5) + "=" + str(d) + '\n'
                                timeLoop6 = timeLoop6 + 1
                                fullLogString = fullLogString + log_string
                            elif key5 in self.oneTimeLogIndex and timeLoop1 > 0:
                                continue
                        else:
                            timeLoop6 = 0
            # INDEX FOR BUCKET
            if self.logElements["index"] == True:
                if "@index-" + bucketName in x["stats"]:
                    x["stats"]["@index-" + bucketName].pop("timestamp")
                    for key6, value5 in x["stats"]["@index-" + bucketName].items():
                        timeLoop7 = 0
                        for e in value5:
                            whatBucket = x["bucket"]
                            if key6 in self.oneTimeLogSys and timeLoop1 == 0:
                                whatBucket = 'sys'  # only do system level logging once
                            elif key6 in self.oneTimeLogSys and timeLoop1 > 0:
                                continue
                            log_string = str(unixTodt_trim[timeLoop7]) + " cb=" + whatBucket + " " + str(key6) + "=" + str(e) + '\n'
                            timeLoop7 = timeLoop7 + 1
                            fullLogString = fullLogString + log_string
                        else:
                            timeLoop7 = 0
            # XDCR FOR BUCKET
            if self.logElements["xdcr"] == True:
                if "@xdcr-" + bucketName in x["stats"]:
                    x["stats"]["@xdcr-" + bucketName].pop("timestamp")
                    for key7, value6 in x["stats"]["@xdcr-" + bucketName].items():
                        timeLoop8 = 0
                        for f in value6:
                            whatBucket = x["bucket"]
                            if key6 in self.oneTimeLogSys and timeLoop1 == 0:
                                whatBucket = 'sys'  # only do system level logging once
                            elif key6 in self.oneTimeLogSys and timeLoop1 > 0:
                                continue
                            log_string = str(unixTodt_trim[timeLoop8]) + " cb=" + whatBucket + " " + str(key7) + "=" + str(f) + '\n'
                            timeLoop8 = timeLoop8 + 1
                            fullLogString = fullLogString + log_string
                        else:
                            timeLoop8 = 0
            # FTS FOR BUCKET
            if self.logElements["fts"] == True:
                if "@fts-" + bucketName in x["stats"]:
                    x["stats"]["@fts-" + bucketName].pop("timestamp")
                    for key8, value7 in x["stats"]["@fts-" + bucketName].items():
                        timeLoop9 = 0
                        for g in value7:
                            whatBucket = x["bucket"]
                            if key8 in self.oneTimeLogSys and timeLoop1 == 0:
                                whatBucket = 'sys'  # only do system level logging once
                            elif key8 in self.oneTimeLogSys and timeLoop1 > 0:
                                continue
                            log_string = str(unixTodt_trim[timeLoop9]) + " cb=" + whatBucket + " " + str(key8) + "=" + str(g) + '\n'
                            timeLoop9 = timeLoop9 + 1
                            fullLogString = fullLogString + log_string
                        else:
                            timeLoop9 = 0
            # Eventing FOR BUCKET
            if self.logElements["eventing"] == True:
                if "@eventing" in x["stats"]:
                    x["stats"]["@eventing"].pop("timestamp")
                    for key9, value8 in x["stats"]["@eventing"].items():
                        timeLoop10 = 0
                        for h in value8:
                            whatBucket = x["bucket"]
                            if key9 in self.oneTimeLogSys and timeLoop1 == 0:
                                whatBucket = 'sys'  # only do system level logging once
                            elif key9 in self.oneTimeLogSys and timeLoop1 > 0:
                                continue
                            log_string = str(unixTodt_trim[timeLoop10]) + " cb=" + whatBucket + " " + str(key9) + "=" + str(h) + '\n'
                            timeLoop10 = timeLoop10 + 1
                            fullLogString = fullLogString + log_string
                        else:
                            timeLoop10 = 0

            # Analytics FOR BUCKET
            if self.logElements["analytics4Bucket"] == True:
                if "@cbas-" + bucketName in x["stats"]:
                    x["stats"]["@cbas-" + bucketName].pop("timestamp")
                    for key10, value9 in x["stats"]["@cbas-" + bucketName].items():
                        timeLoop11 = 0
                        for i in value9:
                            whatBucket = x["bucket"]
                            if key10 in self.oneTimeLogSys and timeLoop1 == 0:
                                whatBucket = 'sys'  # only do system level logging once
                            elif key10 in self.oneTimeLogSys and timeLoop1 > 0:
                                continue
                            log_string = str(unixTodt_trim[timeLoop11]) + " cb=" + whatBucket + " " + str(key10) + "=" + str(i) + '\n'
                            timeLoop11 = timeLoop11 + 1
                            fullLogString = fullLogString + log_string
                        else:
                            timeLoop11 = 0

            # Analytics FOR CLUSTER
            if self.logElements["analytics"] == True:
                if "@cbas" in x["stats"]:
                    x["stats"]["@cbas"].pop("timestamp")
                    for key11, value10 in x["stats"]["@cbas-" + bucketName].items():
                        timeLoop12 = 0
                        for j in value10:
                            whatBucket = x["bucket"]
                            if key11 in self.oneTimeLogAnalytics and timeLoop1 == 0:
                                if key11 in self.oneTimeLogSys and timeLoop1 == 0:
                                    whatBucket = 'sys'  # only do system level logging once
                                elif key11 in self.oneTimeLogSys and timeLoop1 > 0:
                                    continue
                                log_string = str(unixTodt_trim[timeLoop12]) + " cb=sys " + str(key11) + "=" + str(j) + '\n'
                                timeLoop12 = timeLoop12 + 1
                                fullLogString = fullLogString + log_string
                            elif key5 in self.oneTimeLogAnalytics and timeLoop1 > 0:
                                continue
                        else:
                            timeLoop12 = 0



            timeLoop1 = 1 ## this is the counter for after the first time making all the sys stats for a cluster
        self.writeLog(fullLogString)
        return True

    def writeLog(self, log=''):
        today = time.strftime("%Y-%m-%d")
        file = open(self.logPath + today + "_cbstats.txt", "ab")
        file.write(log)
        file.close()

if __name__ == "__main__":
    ''' config = {"hostname":"127.0.0.1","port":"8091","secure":False,"debug":True,"username":"Administrator","password":"password","path":"/tmp/logs/"} '''
    file = open(os.path.dirname(__file__) + "/config.json", "r")
    config = json.loads(file.read())

    a = CBSTATSPULLER(config)
    # b = a.bucketsList()
    # b = a.pullCbBuckets()
    # b = a.pullCbStatus()
    b = a.makeLog()

    '''
	print os.path.dirname(__file__)
	print os.path.dirname(os.path.abspath(sys.argv[0]))
	'''