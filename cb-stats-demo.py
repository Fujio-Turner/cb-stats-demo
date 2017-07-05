#!/usr/bin/python
import json,urllib2,time,datetime,base64,os
#import sys

class CBSTATSPULLER:

	hostname = '127.0.0.1'
	port = '8091'
	nodePort = '8091'
	debug = False
	username = "Administrator"
	password = "password"
	logPath = "/tmp/logs/"
	oneTimeLog = {"cpu_idle_ms","cpu_local_ms","cpu_utilization_rate","curr_connections","mem_used_sys","mem_total","mem_free","mem_actual_free","swap_used","swap_total","hibernated_requests","hibernated_waked","rest_requests"}

	def __init__(self,config):
		self.hostname = config["hostname"]
		self.port     = str(config["port"])
		self.username = config["username"]
		self.password = config["password"]
		self.logPath = config["path"]
		if config["secure"] == True:
			self.secure = "https"
		else:
			self.secure = "http" 
		if config["debug"] == True:
			self.debug = True
		else:
			self.debug = False 

		##--------Common Methods BEGIN---------##
	def httpGet(self,url='',retry=0):

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
							print "DEBUG: HTTP CODE ON: GET - "+ str(e.code)	
						return e.code
					else:
						return False
			time.sleep(1.0)
			return self.httpGet(url,retry+1)

	def unixToDt(self,unix = ''):
		return datetime.datetime.fromtimestamp(int(unix)).strftime('%Y-%m-%d %H:%M:%S')
	
	def jsonChecker(self, data=''):
		#checks if its good json and if so return back Python Dictionary
		try:
			checkedData = json.loads(data)
			return checkedData
		except Exception, e:
			return False

	def sayHelloTest(self):
		print "hello"

		##--------Common Methods END---------##
			
	def pullCbBuckets(self):
		url = self.secure+"://"+self.hostname+":"+self.port+"/pools/default/buckets/"
		if self.debug == True:
			print( "DEBUG: ",url)
		
		data = self.httpGet(url)
		
		if data == False or None:
			now = time.strftime("%Y-%m-%d %H:%M:%S")
			data = now +" error=could not get stats \n"
			self.writeLog(data)
			return False
		
		if self.debug == True:
			print( "DEBUG: ",data)
		return data

	def bucketsList(self):
		bucketsList = self.pullCbBuckets()

		if bucketsList == False:
			exit()

		bucket = []
		nodes  = []
		data   = {}

		for x in bucketsList:
			bucket.append(x["name"])
			'''
			for y in x["nodes"]:
				nodes.append(y["hostname"])
			'''
		data["bucketList"] = bucket
		#data["nodeList"] = nodes

		if self.debug == True:
			print "DEBUG: Bucket List "+ json.dumps(data)
		return data

	def pullCbStatus(self):
		cbList = self.bucketsList()
		dataFull = []
		for x in cbList["bucketList"]:
			url = self.secure+"://"+self.hostname+":"+self.port+"/pools/default/buckets/"+x+"/nodes/"+self.hostname+":"+self.nodePort+"/stats"
			if self.debug == True:
				print( "DEBUG: ",url)
			data = self.httpGet(url)
			if self.debug == True:
				print( "DEBUG: ",json.dumps(data))
			data["bucket"] = x
			dataFull.append(data)
		return dataFull

	def makeLog(self):
		data = self.pullCbStatus()
		fullLogString = ""
		timeLoop1 = 0
		for x in data:
			timeStamp = x["op"]["samples"]["timestamp"]
			unixTodt_trim = []

			for y in timeStamp:
				unixTodt_trim.append(self.unixToDt(y/1000)) #I have to trim the time and convert to human readable
			
			if self.debug == True:
				print("DEBUG: unix to DT conversion ",unixTodt_trim)

			x["op"]["samples"].pop("timestamp") #removes the field timestamp

			for key1, value1 in x["op"]["samples"].items():
					timeLoop2 = 0
					for z in value1:
						whatBucket = x["bucket"]
						if key1 in self.oneTimeLog and timeLoop1 == 0:
							whatBucket = 'sys'  # only do system level logging once
						elif key1 in self.oneTimeLog and timeLoop1 > 0:
							continue
						log_string = str(unixTodt_trim[timeLoop2]) + " cb="+whatBucket+" " + str(key1)+"="+str(z) + '\n'
						timeLoop2 = timeLoop2 + 1
						fullLogString = fullLogString +log_string
					else:
						timeLoop2 = 0
			timeLoop1 = 1
		
		self.writeLog(fullLogString)
		return True

	def writeLog(self,log=''):
		today = time.strftime("%Y-%m-%d")
		file = open(self.logPath+today+"_cbstats.txt","ab") 
		file.write(log) 		 
		file.close() 

if __name__ == "__main__":
	#config = {"hostname":"127.0.0.1","port":"8091","secure":False,"debug":True,"username":"Administrator","password":"password","path":"/tmp/logs/"}
	file = open(os.path.dirname(__file__)+"/config.json", "r") 
	config = json.loads(file.read()) 

	a = CBSTATSPULLER(config)	
	#b = a.bucketsList()
	#b = a.pullCbBuckets()
	#b = a.pullCbStatus()
	b = a.makeLog()

	#print os.path.dirname(__file__)
	#print os.path.dirname(os.path.abspath(sys.argv[0]))
	