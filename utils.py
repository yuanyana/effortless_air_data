# -*- coding: utf-8 -*-
import urllib
import requests
import json
import re
import threading
from airctrl import airctrl
#from smb.SMBHandler import SMBHandler

def isJsonStr(text):
	try:
		json.loads(text)
		return True
	except ValueError:
		return False

def jsonPreprocess(data):
	if type(data) is dict:
		return { k: jsonPreprocess(v) for k, v in data.items() }
	if type(data) is list:
		return [ jsonPreprocess(i) for i in data ]
	if type(data) is str and isJsonStr(data):
		return jsonPreprocess(json.loads(data))
	return data
	#data = re.sub(r'"(-?(\d|[1-9]\d+)(\.(\d|\d+[1-9]))?)"', r"\1", data)
 
def getACStatus(url, ipaddr=None):
	if not ipaddr:
		ipaddr = urllib.parse.urlparse(url).hostname
	try:
		ac = airctrl.AirClient(ipaddr)
		ac.load_key()
		data = ac._get(url)
		data = jsonPreprocess(data)
		return { "status": "OK", "data": data }
	except Exception as e:
		return { "status": "ERROR", "message": str(e) }
		
# def getJsonFromSmb(url):
# 	try:
# 		parsed = urllib.parse.urlparse(url)
# 		if parsed.scheme != "smb":
# 			raise Exception(("\"{}\" is not a samba URL").format(url))
# 		director = urllib.request.build_opener(SMBHandler)
# 		with director.open(url) as fh:
# 			data = json.loads(fh.read().decode("utf-8"))
# 		data = jsonPreprocess(data)
# 		return { "status": "OK", "data": data }
# 	except Exception as e:
# 		return { "status": "ERROR", "message": str(e) }

def getAVStatus(url, ipaddr=None, timeout=10):
	try:
		r = requests.request("GET", url, timeout=timeout)
		if r.status_code != requests.codes.ok:
			raise Exception(r.raise_for_status())
		raw = r.json()
		data = {}
		data["current"] = raw["current"]
		data["outdoor_station"] = raw["historical"]["hourly"][0]["outdoor_station"]
		data["outdoor_weather"] = raw["historical"]["hourly"][0]["outdoor_weather"]
		data = jsonPreprocess(data)
		return { "status": "OK", "data": data }
	except Exception as e:
		return { "status": "ERROR", "message": str(e) }
	#return getJsonFromSmb(url)
	
def getMiDevStatus(miDevice, xHandler=print, xMax=10):
	try:
		miDevice.connection.send({"cmd": "read", "sid": miDevice.sid}, ip=miDevice.gateway.ip)
		for _ in range(xMax):
			data, _ = miDevice.connection.socket.recvfrom(miDevice.connection.SOCKET_BUFSIZE)
			r = json.loads(data.decode("utf-8"))
			r = jsonPreprocess(r)
			conditions = [
                r["cmd"] == "read_ack",
				r["sid"] == miDevice.sid
            ]
			if all(conditions):
				print("Received: %s" % r)
				return { "status": "OK", "data": r["data"] }
			print("Unexpected: %s" % r)
			xHandler(r)
		raise Exception("Not yet received after maximal tries")
	except Exception as e:
		return { "status": "ERROR", "message": str(e) }
		
def setInterval(interval, lock = threading.Lock(), times = None):
	# This will be the actual decorator,
	# with fixed interval and times parameter
	def outer_wrap(function):
		# This will be the function to be
		# called
		def wrap(*args, **kwargs):
			stop = threading.Event()

			# This is another function to be executed
			# in a different thread to simulate setInterval
			def inner_wrap():
				i = 0
				while i is not times and not stop.isSet():
					stop.wait(interval)
					lock.acquire()
					function(*args, **kwargs)
					lock.release()
					i += 1

			t = threading.Timer(0, inner_wrap)
			t.daemon = True
			t.start()
			return stop
		return wrap
	return outer_wrap

def miHubStream(conn):
	while True:
		try:
			data, _ = conn.socket.recvfrom(conn.SOCKET_BUFSIZE)
			payload = json.loads(data.decode("utf-8"))
			payload = jsonPreprocess(payload)
			yield { "status": "OK", "data": payload }
		except Exception as e:
			yield { "status": "ERROR", "message": str(e) }

class queryDED:

	def __init__(self, projectId, webToken):
		# self.baseUrl = "https://canvas-development.data-enabled.com/{query}"
		self.baseUrl = "https://www.hsc.philips.com.cn/data-canvas/{query}"
		self.headers = {
			"projectid": projectId,
			"webtoken": webToken,
			"Content-Type": "application/json",
			"Cache-Control": "no-cache"
		}
		self.timeOut = 10
		
	def listData(self, **query):
		url = self.baseUrl.replace("{query}", "listData")		
		try:
			r = requests.request("GET", url, headers=self.headers, params=query, timeout=self.timeOut)
			if r.status_code != requests.codes.ok:
				raise Exception(r.raise_for_status())
			return { "status": "OK", "data": r.json() }
		except Exception as e:
			return { "status": "ERROR", "message": str(e) }

	def listClusters(self, **query):
		url = self.baseUrl.replace("{query}", "listClusters")
		try:
			r = requests.request("GET", url, headers=self.headers, params=query, timeout=self.timeOut)
			if r.status_code != requests.codes.ok:
				raise Exception(r.raise_for_status())
			return { "status": "OK", "data": r.json() }
		except Exception as e:
			return { "status": "ERROR", "message": str(e) }

	def listDevices(self, **query):
		url = self.baseUrl.replace("{query}", "listDevices")
		try:
			r = requests.request("GET", url, headers=self.headers, params=query, timeout=self.timeOut)
			if r.status_code != requests.codes.ok:
				raise Exception(r.raise_for_status())
			return { "status": "OK", "data": r.json() }
		except Exception as e:
			return { "status": "ERROR", "message": str(e) }

	def storeData(self, **payload):
		url = self.baseUrl.replace("{query}", "storeData")
		try:
			r = requests.request("POST", url, data=json.dumps(payload), headers=self.headers, timeout=self.timeOut)
			if r.status_code != requests.codes.ok:
				raise Exception(r.raise_for_status())
			return { "status": "OK", "data": r.text }
		except Exception as e:
			return { "status": "ERROR", "message": str(e) }
	
	def deleteData(self, dataId):
		url = self.baseUrl.replace("{query}", "deleteData")
		
		payload = { "dataId": dataId }
		
		try:
			r = requests.request("POST", url, data=json.dumps(payload), headers=self.headers, timeout=self.timeOut)
			if r.status_code != requests.codes.ok:
				raise Exception(r.raise_for_status())
			return { "status": "OK", "data": r.text }
		except Exception as e:
			return { "status": "ERROR", "message": str(e) }
