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
	logElements = {"query":True,"kv":True,"sys":True,"fts":True}
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

		#'http://127.0.0.1:8091/_uistats?bucket=todo&haveTStamp={"todo":1499910229945,"@system":1499910229945,"@fts":1499910229945,"@fts-todo":1499910229945,"@index":1499910229945,"@index-todo":1499910229945,"@query":1499910229945,"@xdcr-todo":0}&zoom=minute'	
		for x in cbList["bucketList"]:
			url = self.secure+"://"+self.hostname+":"+self.port+"/_uistats?bucket="+x+"&zoom=minute"
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
			bucketName = x["bucket"]
			timeStamp = x["stats"][bucketName]["timestamp"]
			unixTodt_trim = []
			for y in timeStamp:
				unixTodt_trim.append(self.unixToDt(y/1000)) #I have to trim the time and convert to human readable
			
			if self.debug == True:
				print("DEBUG: unix to DT conversion ",unixTodt_trim)

			x["stats"][bucketName].pop("timestamp") #removes the field timestamp
			
			for key1, value1 in x["stats"][bucketName].items():
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
			
			#QUERY
			if "@query" in x["stats"]:
				x["stats"]["@query"].pop("timestamp")
				for key2, value2 in x["stats"]["@query"].items():
						timeLoop3 = 0
						for zz in value2:
							whatBucket = x["bucket"]
							if key2 in self.oneTimeLog and timeLoop1 == 0:
								whatBucket = 'sys'  # only do system level logging once
							elif key2 in self.oneTimeLog and timeLoop1 > 0:
								continue
							log_string = str(unixTodt_trim[timeLoop3]) + " cb="+whatBucket+" " + str(key2)+"="+str(zz) + '\n'
							timeLoop3 = timeLoop3 + 1
							fullLogString = fullLogString +log_string
						else:
							timeLoop3 = 0

			#FTS 
			if "@fts" in x["stats"]:
				x["stats"]["@fts"].pop("timestamp")
				for key3, value3 in x["stats"]["@fts"].items(): 
						timeLoop4 = 0
						for zzz in value3:
							whatBucket = x["bucket"]
							if key3 in self.oneTimeLog and timeLoop1 == 0:
								whatBucket = 'sys'  # only do system level logging once
							elif key3 in self.oneTimeLog and timeLoop1 > 0:
								continue
							log_string = str(unixTodt_trim[timeLoop4]) + " cb="+whatBucket+" " + str(key3)+"="+str(zzz) + '\n'
							timeLoop4 = timeLoop4 + 1
							fullLogString = fullLogString +log_string
						else:
							timeLoop4 = 0
			#INDEX
			if "@index" in x["stats"]:
				x["stats"]["@index"].pop("timestamp")
				for key4, value4 in x["stats"]["@index"].items(): 
						timeLoop5 = 0
						for zzzz in value4:
							whatBucket = x["bucket"]
							if key4 in self.oneTimeLog and timeLoop1 == 0:
								whatBucket = 'sys'  # only do system level logging once
							elif key4 in self.oneTimeLog and timeLoop1 > 0:
								continue
							log_string = str(unixTodt_trim[timeLoop5]) + " cb="+whatBucket+" " + str(key4)+"="+str(zzzz) + '\n'
							timeLoop5 = timeLoop5 + 1
							fullLogString = fullLogString +log_string
						else:
							timeLoop5 = 0
			#INDEX FOR BUCKET
			if "@index-"+bucketName in  x["stats"]:
				x["stats"]["@index-"+bucketName].pop("timestamp")
				for key5, value5 in x["stats"]["@index-"+bucketName].items(): 
						timeLoop6 = 0
						for zzzzz in value5:
							whatBucket = x["bucket"]
							if key5 in self.oneTimeLog and timeLoop1 == 0:
								whatBucket = 'sys'  # only do system level logging once
							elif key5 in self.oneTimeLog and timeLoop1 > 0:
								continue
							log_string = str(unixTodt_trim[timeLoop6]) + " cb="+whatBucket+" " + str(key5)+"="+str(zzzzz) + '\n'
							timeLoop6 = timeLoop6 + 1
							fullLogString = fullLogString +log_string
						else:
							timeLoop6 = 0
			#XDCR FOR BUCKET
			if "@xdcr-"+bucketName in  x["stats"]:
				x["stats"]["@xdcr-"+bucketName].pop("timestamp")
				for key6, value6 in x["stats"]["@xdcr-"+bucketName].items(): 
						timeLoop7 = 0
						for zzzzzz in value6:
							whatBucket = x["bucket"]
							if key6 in self.oneTimeLog and timeLoop1 == 0:
								whatBucket = 'sys'  # only do system level logging once
							elif key6 in self.oneTimeLog and timeLoop1 > 0:
								continue
							log_string = str(unixTodt_trim[timeLoop7]) + " cb="+whatBucket+" " + str(key6)+"="+str(zzzzzz) + '\n'
							timeLoop7 = timeLoop7 + 1
							fullLogString = fullLogString +log_string
						else:
							timeLoop7 = 0
			#XDCR FOR BUCKET
			if "@fts-"+bucketName in  x["stats"]:
				x["stats"]["@fts-"+bucketName].pop("timestamp")
				for key7, value7 in x["stats"]["@fts-"+bucketName].items(): 
						timeLoop8 = 0
						for zzzzzzz in value7:
							whatBucket = x["bucket"]
							if key7 in self.oneTimeLog and timeLoop1 == 0:
								whatBucket = 'sys'  # only do system level logging once
							elif key7 in self.oneTimeLog and timeLoop1 > 0:
								continue
							log_string = str(unixTodt_trim[timeLoop8]) + " cb="+whatBucket+" " + str(key7)+"="+str(zzzzzzz) + '\n'
							timeLoop8 = timeLoop8 + 1
							fullLogString = fullLogString +log_string
						else:
							timeLoop8 = 0

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
	