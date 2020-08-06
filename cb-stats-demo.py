#!/usr/bin/python
import json
import urllib2
import time
import datetime
import base64
import os
import pstats
import io

class CBSTATSPULLER():
    hostname = '127.0.0.1'
    port = '8091'
    debug = False
    username = "read-only"
    password = "password"
    logPath = "/tmp/logs/"
    defaultDtFormat = "%Y-%m-%d %H:%M:%S"
    logFile = None
    logElements = {"@query":True,"@index":True,"@indexBucket":True,"@system":True,"@kvBucket":True,"@xdcrBucket":True,"@ftsBucket":True,"@fts":True,"@eventing":True,"@cbasBucket":True,"@cbas":True}

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

    def bucketsList(self):
        url = self.secure + "://" + self.hostname + ":" + self.port + "/pools/default/buckets?basic_stats=true&skipMap=true"
        data = self.httpGet(url)
        if self.debug == True:
            print "DEBUG: Bucket List " + json.dumps(data)
        bucket = []
        for x in data:
            bucket.append(x["name"])
        return bucket

    def pullCbStatus(self,bucket="default"):
        url = self.secure + "://" + self.hostname + ":" + self.port + "/_uistats?bucket=" + bucket + "&zoom=minute"
        if self.debug == True:
            print("DEBUG: ", url)
        data = self.httpGet(url)
        if self.debug == True:
            print("DEBUG: ", json.dumps(data))
        return data

    #@profile
    def makeLog(self):
        self.writeLogOpen()
    
        ### Bucket Operations
        cbList = self.bucketsList()

        if self.debug == True:
            print("DEBUG: BucketList ", cbList)

        if cbList > 0:
            if self.debug == True:
                print("DEBUG: Making bucket logs ")
            for bucketName in cbList:
                bData = self.pullCbStatus(bucketName) 
                self.StatsB(bucketName,bData["stats"])

            if self.debug == True:
                print("DEBUG: Making system logs on:",cbList[0])
            bData = self.pullCbStatus(cbList[0])
            self.StatsC(bData["stats"])

        else:
            now = time.strftime(self.defaultDtFormat)
            data = now + " error=No Buckets \n"
            self.writeLogWrite(data)

        self.writeLogClose()
        return True

    def StatsB(self,bucketName,data):
        for key , value in data.items():
            dType = key.split("-")
            if len(dType)>1 and dType[1] == bucketName:
                if self.logElements[dType[0]+"Bucket"] == True:
                    timeStamp = data[key]["timestamp"]
                    value.pop("timestamp")
                    for key2, value2 in value.items():
                        timeLoop = 0
                        for a in value2:
                            log_string = str(self.unixToDt(timeStamp[timeLoop]/1000)) + " cb="+bucketName +" " + key2 + "=" + str(a) + '\n'
                            self.writeLogWrite(log_string)
                            timeLoop += 1

    def StatsC(self,data):
        for key , value in data.items():
            dType = key.split("-")
            if len(dType) == 1 :
                if self.logElements[dType[0]] == True:
                    timeStamp = data[key]["timestamp"]
                    value.pop("timestamp")
                    for key2, value2 in value.items():
                        timeLoop = 0
                        for a in value2:
                            log_string = str(self.unixToDt(timeStamp[timeLoop]/1000)) + " cb=sys " + key2 + "=" + str(a) + '\n'
                            self.writeLogWrite(log_string)
                            timeLoop += 1

    def writeLogOpen(self, log=''):
        today = time.strftime("%Y-%m-%d")
        self.logFile = open(self.logPath + today + "_cbstats.txt", "ab")

    def writeLogWrite(self, log=''):
        self.logFile.write(log)

    def writeLogClose(self, log=''):
        self.logFile.close()

if __name__ == "__main__":
    file = open(os.path.dirname(__file__) + "/config.json", "r")
    config = json.loads(file.read())
    a = CBSTATSPULLER(config)
    b = a.makeLog()