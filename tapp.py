__author__ = 'Aji John, Kristiina Ausmees'
import json
import os
from celery import Celery
from celery import group
import re
import time
import tempfile
import sqlite3
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import requests
from datetime import datetime
import sys
import subprocess
from kombu.common import Broadcast, Queue, Exchange
import logging
import socket
import ConfigParser
import random



#Initialize the Config
config = ConfigParser.SafeConfigParser({})
config.read('config.cfg')

'''
The config file: must be called dfaas.cfg and be in the same directory
Below is a sample config to get an idea
[bamsi]
CELERY_BROKER_URL = amqp://celery:stalk@MASTER_NODE/celery-host
CELERY_RESULT_BACKEND = amqp
DATA_PATH=ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/data/{individual}/alignment/{filename}
MASTER_IP=MASTER_NODE
MASTER_PORT=8888

[storage]
WEBHDFS_IP=STORAGE_NODE
WEBHDFS_PORT=50070
WEBHDFS_PUBLIC_IP=STORAGE_NODE
WEBHDFS_PUBLIC_PORT=14000
WEBHDFS_USER=ubuntu
RESULTS_PATH=/filtered/

----
'''
class Config:
	CELERY_BROKER_URL = config.get('bamsi','CELERY_BROKER_URL')
	CELERY_RESULT_BACKEND = config.get('bamsi','CELERY_RESULT_BACKEND')
	MASTER_IP= config.get('bamsi','MASTER_IP')
	MASTER_PORT= config.get('bamsi','MASTER_PORT')
	DATA_PATH = config.get('bamsi', 'DATA_PATH')
	CELERY_QUEUES = (Queue('default', Exchange('default'), routing_key='default'), Broadcast('q1'), )
	CELERY_ROUTES = {'tapp.readBAM': 'default','tapp.readMock': {'queue': 'q1'}}
	NO_OF_ROWS = 3000


# Initialize Celery
celery = Celery('tapp',backend=Config.CELERY_RESULT_BACKEND, broker=Config.CELERY_BROKER_URL)
celery.config_from_object(Config)

#Logging config
logger = logging.getLogger('bamsi')
fhdlr = logging.FileHandler('bamsi.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
fhdlr.setFormatter(formatter)
logger.addHandler(fhdlr)
logger.setLevel(logging.DEBUG)

#broadcast
celery.conf.task_queues = (Broadcast('broadcast_tasks'),)
celery.conf.task_routes = {
	'tapp.readMock': {
		'queue': 'broadcast_tasks',
		'exchange': 'broadcast_tasks'
	}
}



class StorageRepositoryBase(object):
	""" Template class for class implementing interaction with storage repository.

	Implement your own version of this class for your storage repository of choice.


    """
	def __init__(self):
		"""Initialize the storage repository class.

		Returns:
			 nothing
		"""
		pass


	def push_to_storage(self, file_path_local, group_id, file_name):
		"""Push a file from local machine to storage repository.

		Args:
			file_path_local (str): total path of file on local machine.
			group_id (str): id  of the group the file belongs to (eg job tracking id).
			file_name (str): name the file will have in the storage repository.


		Returns:
			 nothing
		"""
		raise NotImplementedError



	def get_download_urls(self, trackingID, fileIDs):
		"""Get URLs from which the selected files can be downloaded (by the client) from the storage repository.

		Args:
			trackingID (str): ID of the query the files belong to.
			fileIDs (str list): list of IDs of the files to be downloaded (individual ID in case of 1000 genomes data).


		Returns:
			 (URLlist, totalsize)  (str list, int)
			 URLs that the set of selected files can be downloaded from.
			 total size in bytes of all files to be downloaded.

		This function is only required if the server is to support downloading results via HTTP.
		"""

		raise NotImplementedError


class HDFS(StorageRepositoryBase):
	def __init__(self):
		"""Initialize the HDFS storage repository class.
		   Reads settings from section 'storage' in a ConfigParser object named config.
		"""

		self.WEBHDFS_IP=config.get('storage','WEBHDFS_IP')
		self.WEBHDFS_PUBLIC_IP=config.get('storage','WEBHDFS_PUBLIC_IP')
		self.WEBHDFS_PORT=config.get('storage','WEBHDFS_PORT')
		self.WEBHDFS_PUBLIC_PORT=config.get('storage','WEBHDFS_PUBLIC_PORT')
		self.WEBHDFS_USER=config.get('storage','WEBHDFS_USER')
		self.RESULTS_PATH = config.get('storage', 'RESULTS_PATH')
		StorageRepositoryBase.__init__(self)

	def push_to_storage(self, file_path_local, group_id, file_name):
		file_path_hdfs = '{dir}{subdir}/{file}'.format(dir=self.RESULTS_PATH, subdir=group_id, file=file_name)
		command1 = ["curl","-sS","-f","-i", "-X", "PUT", "http://"+self.WEBHDFS_IP+":"+self.WEBHDFS_PORT+"/webhdfs/v1"+
						   file_path_hdfs + "?user.name="+self.WEBHDFS_USER+"&op=CREATE"]


		put1 = subprocess.Popen(command1, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		(out, err) = put1.communicate()

		if put1.returncode != 0:
			raise RuntimeError(err)

		response_location = filter(lambda x: x.startswith('Location:'), out.split("\n"))[0].split(" ")[1].rstrip()

		command2 = ["curl",'-H', 'Content-Type: application/octet-stream', "-X", "PUT", "-T", file_path_local, response_location]
		put2 = subprocess.Popen(command2, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		(out, err) = put2.communicate()

		if put2.returncode != 0:
			raise RuntimeError(err)


	def get_download_urls(self, trackingID, fileIDs):
		payload = {'user.name': self.WEBHDFS_USER, 'op': 'LISTSTATUS'}
		r = requests.get("http://"+self.WEBHDFS_IP+":"+self.WEBHDFS_PORT+"/webhdfs/v1/filtered/"+trackingID, params=payload)
		totalsize = 0
		URLlist = []

		if r.status_code == 200:
			files = r.json()['FileStatuses']['FileStatus']

			for file in files:
				filesize = file['length']
				filename = file['pathSuffix']
				fileID = filename.split(".")[0]
				if fileID in fileIDs:
					totalsize += int(filesize)
					URLlist.append("http://"+self.WEBHDFS_PUBLIC_IP+":"+self.WEBHDFS_PUBLIC_PORT+"/webhdfs/v1/filtered/"+trackingID+"/"+filename+"?user.name="+self.WEBHDFS_USER+"&op=OPEN")

		else:
			logger.info("Fetching download URLs failed: " + r.text)


		return (URLlist, totalsize)


#Initialize storage repository
StorageRepository = HDFS()

class Application(tornado.web.Application):
	def __init__(self):
		handlers = [
			(r"/", InfoHandler),
			(r"/launch", IndexHandler),
			(r"/dash", DashHandler),
			(r"/populate", PopulateHandler),
			(r"/resubmit/(\w+)", BAMReHandler),
			(r"/spawn", BAMJobHandler),
			(r"/jobs", JobsDumpHandler),
			(r"/jobstatus", JobsStatusHandler),
			(r"/status", JobStatusHandler),
			(r"/update", JobUpdateHandler),
			(r"/runs", RunStatusHandler),
			(r"/hello", HelloHandler),
			(r"/poke", PokeHandler),
			(r"/download",DownloadHandler)

		]
		settings = dict(
			app_title=u"1K Controls",
			template_path=os.path.join(os.path.dirname(__file__), "templates"),
			static_path=os.path.join(os.path.dirname(__file__), "static"),
			xsrf_cookies=True,
			cookie_secret="__godzilla__",
			debug=True,
		)
		super(Application, self).__init__(handlers, **settings)


#For checking the workers
class PokeHandler(tornado.web.RequestHandler):
	def get(self):
		i = celery.control.inspect()
		activeworkers = i.active()
		self.set_header('Content-Type', 'application/javascript')
		self.write(json.dumps(activeworkers))

#For checking status of a worker
class JobStatusHandler(tornado.web.RequestHandler):
	def get(self):
		trackingID = self.get_argument('tracking')

		result = celery.AsyncResult(trackingID)

		self.set_header('Content-Type', 'application/javascript')
		self.write(json.dumps({"TrackingId":trackingID,"Status":result.state}))

#For doing health check
class HelloHandler(tornado.web.RequestHandler):
	def get(self):
		retStuff = []

		nodes = activeNodes()

		#Health check
		retStuff.append({"MyIP": Config.MASTER_IP, "MyPort":Config.MASTER_PORT ,
									"Workers": len(nodes), "weather": "Sunny"})

		self.set_header('Content-Type', 'application/javascript')
		self.write(json.dumps(retStuff))


#For Resubmitting the failed jobs
class BAMReHandler(tornado.web.RequestHandler):
	def get(self,individual):
		conn = sqlite3.connect('1kg.db')
		nfs = self.get_argument('nfs')
		fileProcessed=[]
		bam_files = []
		trackingID = self.get_argument('tracking', None)

		with conn:

			cur = conn.cursor()

			cur.execute("SELECT * FROM fileshist WHERE individual=:individual ",
					{"individual": individual})

			conn.commit()



			rows = cur.fetchall()
			for row in rows:
				logger.info( "%s %s" % (row[0], row[1]))

				# if nfs is indicated, work with the local nfs files
				if nfs=='yes':
					bam_files.append(row[3])
				else:
					bam_files.append(row[2])

				fileProcessed.append(row[5])



		#submit the job
		job = group([readBAM.s(bamfile) for bamfile in bam_files])
		a_result = job.apply_async()

		filesString = ",".join(fileProcessed )

		cur = conn.cursor()
		# update the task id
		cur.execute("UPDATE fileshist SET tracking='" + a_result.id + "',status='DISPATCHED' WHERE individual in (%s)" %
							   ','.join('?'*len(fileProcessed)), fileProcessed)

		conn.commit()
		conn.close()

		self.write('Re-Submitted the job with Tracking Id.' + a_result.id)


class JobUpdateHandler(tornado.web.RequestHandler):
	def get(self):
		filepath = self.get_argument('filepath')
		state = self.get_argument('state')
		msg = self.get_argument('msg')
		filtercount = self.get_argument('filtercount')
		rowcount = self.get_argument('rowcount')
		trackingID = self.get_argument('group', None)
		taskID = self.get_argument('task', None)
		worker = self.get_argument('worker', None)
		runtime = self.get_argument('runtime', None)

		conn = sqlite3.connect('1kg.db')
		cur = conn.cursor()

		today = datetime.today()
		now = today.strftime("%Y-%m-%d %H:%M:%S")


		# (tracking_id, filename) is the key that uniquely defines a particular task - update that entry in fileshist
		file = filepath.split("/")[-1]
		cur.execute("UPDATE fileshist SET status=?,msg=?,filteredcount=? ,updatedate=? ,rowcount=? ,taskid = ?, worker=? , runtime= ? WHERE tracking=? AND filename = ? ", (state,msg,filtercount,now,rowcount,taskID,worker,runtime, trackingID, file))

		conn.commit()
		conn.close()



class RunStatusHandler(tornado.web.RequestHandler):
	def get(self):

		conn = sqlite3.connect('1kg.db')
		conn.row_factory = sqlite3.Row # This enables column access by name: row['column_name']

		trackingID = self.get_argument('tracking', None)

		c = conn.cursor()
		queryString="select count(*) as noofinds, subpop, sum(filteredcount) as totfilter, \
					 sum(rowcount) as totrows from fileshist WHERE tracking=? group by subpop"

		rows = c.execute(queryString, (trackingID,)).fetchall()
		noOfRows = len(rows)

		rowsInJSON =  [dict(ix) for ix in rows]  #CREATE JSON



		self.set_header('Content-Type', 'application/javascript')
		self.write(json.dumps(rowsInJSON))

class JobsStatusHandler(tornado.web.RequestHandler):
	def get(self):

		conn = sqlite3.connect('1kg.db')
		conn.row_factory = sqlite3.Row # This enables column access by name: row['column_name']

		trackingID = self.get_argument('tracking', None)

		c = conn.cursor()
		queryString="select 'READY' as status , count(*) as jobs from fileshist where tracking=? and status='READY' \
					 union select 'DISPATCHED' as status, count(*) as jobs from fileshist where tracking=? and status='DISPATCHED'  \
					 union select 'ERROR' as status, count(*) as jobs from fileshist where tracking=? and status='ERROR' \
					 union select 'COMPLETED' as status, count(*) as jobs from fileshist where tracking=? and status='COMPLETED'  \
					 union select 'STARTED' as status, count(*) as jobs from fileshist where tracking=? and status='STARTED'  \
					 union select 'TOTAL' as status, count(*) as jobs from fileshist where tracking=?  "

		rows = c.execute(queryString,(trackingID,trackingID,trackingID,trackingID,trackingID,trackingID,)).fetchall()
		noOfRows = len(rows)
		retStuff = []

		ret =''

		for row in rows:
			logger.info( "%s %s" % (row[0], row[1]))
			retStuff.append( {row[0] : row[1] })
			ret+= '"' + row[0] +'":' +  str(row[1]) +','



		conn.commit()
		conn.close()

		stripcomma = ret.rfind(',')
		rowsInJSON = json.loads('{'+ ret[:stripcomma] + '}')

		#rowsInJSON =  [dict(ix) for ix in retStuff]  #CREATE JSON

		self.set_header('Content-Type', 'application/javascript')
		self.write(rowsInJSON)

class JobsDumpHandler(tornado.web.RequestHandler):
	def get(self):
		conn = sqlite3.connect('1kg.db')
		conn.row_factory = sqlite3.Row # This enables column access by name: row['column_name']

		trackingID = self.get_argument('tracking', None)
		fields = self.get_arguments('fields')

		c = conn.cursor()

		if(fields) :
			selected_fields = ','.join(fields)
		else:
			selected_fields = '*'

		if trackingID:
			#comma to make your parameter a tuple, as the tracking id is 36 chars long
			rows  = c.execute("SELECT "+selected_fields+" from fileshist WHERE tracking=?", (trackingID,)).fetchall()
		else:
			rows = []

		noOfRows = len(rows)
		conn.commit()
		conn.close()


		rowsInJSON =  [dict(ix) for ix in rows]  #CREATE JSON
		retStuff = []

		retStuff.append({"per_page": 100, "total_entries":noOfRows ,
									"total_pages": noOfRows/100, "page": 1})

		retStuff.append(rowsInJSON)
		self.set_header('Content-Type', 'application/javascript')
		self.write(json.dumps(retStuff))

class DashHandler(tornado.web.RequestHandler):
	def get(self):
		tracking_id = self.get_arguments("tracking")
		nodes=[]
		# TODO Might be an expensive Operation - Rework to be static
		nodes = activeNodes()
		self.render("main.html",nodes=nodes)

class IndexHandler(tornado.web.RequestHandler):
	def get(self):
		nodes=[]
		nodes = activeNodes()
		self.render("index.html",nodes=nodes, info=[])

class InfoHandler(tornado.web.RequestHandler):
	def get(self):
		nodes=[]
		nodes = activeNodes()
		self.render("info.html",nodes=nodes, info=[])

class PopulateHandler(tornado.web.RequestHandler):
	def get(self):

		conn = sqlite3.connect('1kg.db')
		c = conn.cursor()

		# Create table
		try:
			try:
				c.execute('''drop TABLE files''')
				c.execute('''drop TABLE fileshist''')
			except:
				pass
			c.execute('''CREATE TABLE files
				 (id numeric, rundate text, filename text,individual text, subpop text, pop text,status text,  msg text, filteredcount real,taskid text, updatedate text,rowcount real,tracking text,worker text, runtime real)''')
			# create the historical file
			c.execute('''CREATE TABLE fileshist
						 (id numeric, rundate text, filename text,individual text, subpop text, pop text,status text,  msg text, filteredcount real,taskid text, updatedate text,rowcount real,tracking text,worker text, runtime real)''')
		except:
			pass
		finally:
			logger.info("Created/Updated the file" )

		count = 0
		id = 0
		with open('alignment.index', 'r') as f:
			data = f.readlines()
			for line in data:
				words = line.split()
				if ".mapped." in words[0]:
					index = words[0].rfind("/")
					chopped = words[0][index+1:]
					filenamesplit = chopped.split('.')

					'''
					id
					rundate
					filename
					individual
					subpop
					pop
					status
					msg
					filteredcount
					taskid
					updatedate
					rowcount
					tracking
					worker
					runtime
					'''
					today = datetime.today()
					id = id + 1

					queryString="INSERT INTO files VALUES ( '{0}' ,'{1}' , '{2}' ,'{3}','{4}','','READY','',0,'','',0,'','',0)".format(
						id,
						today.strftime("%Y-%m-%d %H:%M:%S"),
						chopped,
						filenamesplit[0],
						filenamesplit[4])

					count = count + 1
					logger.info(queryString)
					# Insert a row of data
					c.execute(queryString)

		conn.commit()
		conn.close()

		self.write("{0} Files inserted".format(count))

class DownloadHandler(tornado.web.RequestHandler):
	def get(self):

		trackingID = self.get_argument('tracking', None)
		fileIDs = self.get_argument('ids', None).split(',')

		(URLlist, total_size) = StorageRepository.get_download_urls(trackingID, fileIDs)


		rowsInJSON = json.dumps({"tracking":trackingID,"URLs":URLlist, "size": total_size})

		self.set_header('Content-Type', 'application/javascript')
		self.write(rowsInJSON)
		# return rowsInJSON

# Get List of Queues
def activeQueues():
	qlist = []
	try:
		actQs = celery.control.inspect().active_queues()
		# get list of Keys - name value pair
		actQKeys = actQs.keys()
		for index, actQ in enumerate(actQs):
			qlist.append(actQs[actQKeys[index]][0]['name'])
	except:
		pass
	return qlist


# Returns list of Active Nodes
def activeNodes():
	i = celery.control.inspect()
	try:
		activeworkers = i.active()
		result = activeworkers.keys()
	except:
		result = []
	return result



all_1kg_regs = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10","11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "X", "Y", "MT", "NC_007605", "hs37d5", "GLs","GL000207.1" , "GL000226.1" , "GL000229.1" , "GL000231.1" , "GL000210.1" , "GL000239.1" , "GL000235.1" , "GL000201.1" , "GL000247.1" , "GL000245.1" , "GL000197.1" , "GL000203.1" , "GL000246.1" , "GL000249.1" , "GL000196.1" , "GL000248.1" , "GL000244.1" , "GL000238.1" , "GL000202.1" , "GL000234.1" , "GL000232.1" , "GL000206.1" , "GL000240.1" , "GL000236.1" , "GL000241.1" , "GL000243.1" , "GL000242.1" , "GL000230.1" , "GL000237.1" , "GL000233.1" , "GL000204.1" , "GL000198.1" , "GL000208.1" , "GL000191.1" , "GL000227.1" , "GL000228.1" , "GL000214.1" , "GL000221.1" , "GL000209.1" , "GL000218.1" , "GL000220.1" , "GL000213.1" , "GL000211.1" , "GL000199.1" , "GL000217.1" , "GL000216.1" , "GL000215.1" , "GL000205.1" , "GL000219.1" , "GL000224.1" , "GL000223.1" , "GL000195.1" , "GL000212.1" , "GL000222.1" , "GL000200.1" , "GL000193.1" , "GL000194.1" , "GL000225.1" , "GL000192.1"]

class SAMFilter:
	""" Represents a set of filter conditions to be applied to a specific SAM/BAM file.

		This class assumes that the filenames are those of the 1000 Genomes project.
    """


	def __init__(self, filter_conditions, filename):
		"""Initiate the filter class  with a set of filter conditions and a SAM/BAM file.

		Args:
			filter conditions (dict):  Dictionary containing the filter conditions.
			filename (str) : absolute path and name of the SAM/BAM file

    """
		self.filter_conditions = filter_conditions
		self.region = re.search('bwa.(.+?).low', filename).group(1)
		ind = re.search('(.+?).mapped', filename).group(1)
		indvindex = ind.rfind("/")
		self.filename = filename
		self.ind =  ind[indvindex+1:]
		self.no_matching_alignments = 0
		self.no_total_alignments = 0
		if "minTlen" in filter_conditions:
			self.minTlen = int(filter_conditions["minTlen"][0])
		else:
			self.minTlen = None
		if "maxTlen" in filter_conditions:
			self.maxTlen = int(filter_conditions["maxTlen"][0])
		else:
			self.maxTlen = None
		if "seq" in filter_conditions:
			self.seq = filter_conditions["seq"][0]
		else:
			self.seq = None

		if "tags_incl" in filter_conditions:
			tags_incl = []
			tags_to_incl = self.filter_conditions["tags_incl"][0].split(",")
			for tag_to_require in tags_to_incl:
				tags_incl.append(re.compile(tag_to_require))
			self.tags_incl = tags_incl
		else:
			self.tags_incl = None

		if "tags_excl" in self.filter_conditions:
			tags_excl = []
			tags_to_excl = self.filter_conditions["tags_excl"][0].split(",")
			for tag_to_excl in tags_to_excl:
				tags_excl.append(re.compile(tag_to_excl))
			self.tags_excl = tags_excl
		else:
			self.tags_excl = None

		if "cigar" in self.filter_conditions:
			self.cigar = re.compile(self.filter_conditions["cigar"][0])
		else:
			self.cigar = None


		if "format" in self.filter_conditions:
			self.format = filter_conditions["format"][0]
		else:
			self.format = "b"


	def get_input_cmd(self):
		"""Get the samtools command to fetch alignments from SAM/BAM file.

		Returns:
			str list : list containing the elemets of the command

   		"""
		cmd = ['samtools', 'view']
		#dont include header if output is reformatted
		if self.filter_conditions["format"][0] != "reformat":
			cmd.append('-h')

		if "f" in self.filter_conditions:
			cmd.append("-f " + self.filter_conditions["f"][0])

		if "F" in self.filter_conditions:
			cmd.append("-F " + self.filter_conditions["F"][0])

		if "q" in self.filter_conditions:
			cmd.append("-q " + self.filter_conditions["q"][0])

		cmd.append(self.filename)

		if "regions" in self.filter_conditions:
			for reg in self.filter_conditions["regions"]:
				# GLs indicates we need to add every individual GL00x to the region-string
				if("GLs" in reg):
					start = ""
					end = ""
					# parse for start-end positions
					if(len(reg.split(":")) > 1):
						rest = reg.split(":")[1]
						start =  ":" + rest.split("-")[0]
						if(len(rest.split("-")) > 1):
							end = "-"+rest.split("-")[1]

					gls_formatted = ""
					for r in all_1kg_regs:

						if "GL00" in r:
							gls_formatted = gls_formatted + r + start + end + " "

					cmd.append(gls_formatted)
				else:
					cmd.append(reg)

		return cmd

	def get_output_filename(self, input_file_name):
		"""Get the filename to be written to.

			Returns:
				str : input_file_name with file ending adjusted to the chosen format.

  		"""
		if self.filter_conditions["format"][0] == "reformat":
			return input_file_name[:-3] + 'gz'
		else:
			return input_file_name


	def get_output_cmd(self):
		""""Get the command used to write to output pipe.

		Returns:
			str list : list containing the elemets of the command

   		 """
		if self.filter_conditions["format"][0] == "reformat":
			return ['gzip']
		else:
			return ['samtools', 'view', '-Shb' , '-']

	def check_conds(self, row):
		"""Check if an alignment fulfils the filter conditions.

		Args:
			row (str): The alignment.

		Returns:
			A formatted string representation of the row if it fulfils self.filter_conditions
			None otherwise.
    	"""


		# If the row is a header line
		if row.startswith('@'):
			return row
		else:
			self.no_total_alignments += 1
			row_vals=row.split()

			if self.minTlen:
				if abs(int(row_vals[8])) < self.minTlen:
					return None
			if self.maxTlen:
				if abs(int(row_vals[8])) > self.maxTlen:
					return None
			if self.seq:
				if(self.seq not in row_vals[9]):
					return None

			if self.tags_incl:
				tags_found = row_vals[11:]
				for tag_to_require in self.tags_incl:
					found = False
					for tag_field in tags_found:
						test = tag_to_require.match(tag_field)
						if test:
							found = True
							break
					if found == False:
						return None

			if self.tags_excl:
				tags_found = row_vals[11:]
				for tag_to_excl in self.tags_excl:
					for tag_field in tags_found:
						test = tag_to_excl.match(tag_field)
						if test:
							return None

			if self.cigar:
				test = self.cigar.match(row_vals[5])
				if not test:
					return None

			self.no_matching_alignments +=1
			if self.format == "reformat":
				return self.ind+"\t"+self.region+"\t" + row

			return row

	@staticmethod
	def check_validity(filter_conds):
		"""Check if a set of filter conditions is valid.

		Args:
			filter_conds (dict): Dictionary containing the filter conditions.

		Returns:
			True if filter_conds are a valid set of filter condtions.
    	"""

		for cond in ["minTlen", "maxTlen", "f", "F", "q"]:
			if cond in filter_conds and not filter_conds[cond][0].isdigit():
				return False
		if "minTlen" in filter_conds and "maxTlen" in filter_conds and int(filter_conds["minTlen"][0]) > int(filter_conds["maxTlen"][0]):
			return False
		if "regions" in filter_conds:
			for reg in filter_conds["regions"]:
				chrom = reg.split(":")[0]
				if not chrom in all_1kg_regs:
					return False
				if "-" in reg:
					min = reg.split(":")[1].split("-")[0]
					max = reg.split(":")[1].split("-")[1]
					if not min.isdigit() or not (max == '' or max.isdigit()):
						return False
				else:
					if not(len(reg.split(":")) == 1 or reg.split(":")[1].isdigit() or reg.split(":")[1] == ''):
						return False

		return True


@celery.task(bind=True,trail=True)
def readMock(self):
	logger.info("<--- Mock task started ---> " )
	logger.info("<--- Mock task completed ---> " )

@celery.task(bind=True,trail=True)
def execStats(self,taskId):
	logger.info("<--- Stats task started ---> " )
	rawLogs = subprocess.Popen(['grep', taskId, 'celery.out'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	stdout, stderr = rawLogs.communicate()

	logger.info(socket.gethostname())
	'''
	trackingID
	stdout
	msg
	worker
	'''


	payload = {'trackingID': taskId, 'stdout': stdout, 'msg': '', 'worker': socket.gethostname()}
	r = requests.get("http://" + Config.MASTER_IP + ":" + Config.MASTER_PORT + "/stats", params=payload)
	logger.info("<--- Stats task completed ---> ")

	return stdout

# Reads the BAM file
# Writes to storage
@celery.task(bind=True,trail=True, max_retries=50, acks_late=True)
def readBAM(self, input_file_name, filter_conds):
	start = time.time()
	msgCount=0
	rowCount = 0
	error_flag = False

	try:
		ind_name = input_file_name.split('.')[0]

		if(self.request.retries <= self.max_retries * 0.8):
			input_file_path = Config.DATA_PATH.format(individual=ind_name, filename=input_file_name)
		else:
			input_file_path = "http://s3.amazonaws.com/1000genomes/phase3/data/{individual}/alignment/{filename}".format(individual=ind_name, filename=input_file_name)


		logger.info("Begin processing task " +  str(self.request.id) +  " of group " + str(self.request.group) + " from " + input_file_path)

		filterer = SAMFilter(filter_conds, input_file_path)

		output_file_name = filterer.get_output_filename(input_file_name)
		local_file = tempfile.NamedTemporaryFile()

		input_cmd = filterer.get_input_cmd()
		input_proc = subprocess.Popen(input_cmd,
							   stdout=subprocess.PIPE, stderr=subprocess.PIPE)


		output_cmd = filterer.get_output_cmd()
		output_proc = subprocess.Popen(output_cmd,
							   stdin=subprocess.PIPE,
							   stdout=local_file,
							   stderr=subprocess.PIPE,
							   shell = False)




		output_stream = output_proc.stdin

		for line in input_proc.stdout:
			rowCount+=1
			result = filterer.check_conds(line)
			if result != None:
				output_stream.write(result)
				output_stream.flush()

			msgCount += 1
			# update msg
			if msgCount == 2000000:
				self.update_state(state='PROGRESS',
					meta={'File': input_file_name,'Rows': rowCount, 'count': filterer.no_matching_alignments,
								'status': 'chugging along!' })
				payload = {'filepath': input_file_path, 'state': 'PROGRESS', 'msg': '', 'filtercount': filterer.no_matching_alignments, 'rowcount':rowCount, 'group':self.request.group, 'task':self.request.id, 'worker':socket.gethostname(), 'runtime':time.time() - start}
				r = requests.get("http://" + Config.MASTER_IP + ":" + Config.MASTER_PORT + "/update", params=payload)
				msgCount = 0

		(out, err_output) = output_proc.communicate()
		(out, err_input) = input_proc.communicate()

		if(input_proc.returncode != 0):
			raise RuntimeError(err_input)

		if(output_proc.returncode != 0):
			raise RuntimeError(err_output)


		logger.info("Finished filtering. Created temp file  " + local_file.name + " with  " + str(filterer.no_matching_alignments) + " alignments")

		write_success = False
		for attempt in range(50):
			try:
				time.sleep(random.randint(1,8))
				StorageRepository.push_to_storage(local_file.name, self.request.group, output_file_name)
				logger.info("Wrote " + local_file.name + " to storage on attempt: " + str(attempt))
				write_success = True
				break
			except Exception:
				exceptMessage = str(sys.exc_info()[0]) + str(sys.exc_info()[1])
				if not exceptMessage:
					exceptMessage= 'Error in writing to storage'
				logger.info("Error in writing to storage repository : {0} on attempt: {1}".format(exceptMessage, str(attempt)))

		if(write_success):
			# Update the Master with the update - COMPLETE
			file_stats = os.stat(local_file.name)
			file_size = file_stats.st_size
			payload = {'filepath': input_file_path, 'state': 'COMPLETED', 'msg': file_size, 'filtercount': filterer.no_matching_alignments, 'rowcount':rowCount, 'group':self.request.group, 'task':self.request.id, 'worker':socket.gethostname(), 'runtime':time.time() - start}
			r = requests.get("http://" + Config.MASTER_IP + ":" + Config.MASTER_PORT + "/update", params=payload)
			state = "COMPLETED"
		else:
			error_flag = True

		local_file.close()
		logger.info("Removed temp file." )

	except Exception as e:
		exceptMessage = str(sys.exc_info()[0]) + str(sys.exc_info()[1])
		if not exceptMessage:
			exceptMessage= 'Error in filtering'
		logger.info("Error in filtering : {0} ".format(exceptMessage))
		error_flag = True
		pass

	elapsed_time = time.time() - start

	if(error_flag):
		retries_done = self.request.retries
		logger.info("Currently done: " + str(retries_done) + " retries")

		# If it will be retried again
		if(retries_done < self.max_retries):
			state = "RETRY"
		else:
			state = "ERROR"

		logger.info(state+": time = " + str(elapsed_time) + "  msg = " + str(sys.exc_info()[1]))
		payload = {'filename': input_file_name, 'state': state, 'msg': exceptMessage, 'filtercount': 0, 'rowcount': 0, 'group':self.request.group, 'task':self.request.id, 'worker':socket.gethostname(), 'runtime':time.time() - start}
		r = requests.get("http://" + Config.MASTER_IP + ":" + Config.MASTER_PORT + "/update", params=payload)

		raise self.retry(countdown = random.randint(5,10))

	ret_msg = []
	ret_msg.append({'BAM File': input_file_name, 'filteredOut': filterer.no_matching_alignments,
				'timeTaken':elapsed_time,'status': state})

	logger.info("Ended processing task " +  str(self.request.id) +  " of group " + str(self.request.group) + " time = " + str(elapsed_time) + " filtered out = " + str(filterer.no_matching_alignments) + " state = " + state)
	return ret_msg


class BAMJobHandler(tornado.web.RequestHandler):
	def get(self):
		bam_files = []
		conn = sqlite3.connect('1kg.db')
		query_args = self.request.query_arguments
		selected_individuals=[]

		if "num_files" in query_args:
			limit = query_args["num_files"][0]
		else:
			limit = str(Config.NO_OF_ROWS)

		with conn:
			cur = conn.cursor()
			#If any subpopulations chosen, then limit the query to these
			if "subpops" in query_args:
				if "api_key" in query_args:
					logger.info(query_args["subpops"])
					subpops = query_args["subpops"][0].split(",")
				else:
					subpops = query_args["subpops"]
				cur.execute("SELECT * FROM files WHERE subpop in (" +",".join(["?"]*len(subpops))+")" + " limit "+limit, subpops)

			if "inds" in query_args:
				inds = query_args["inds"][0].replace(" ", "").split(',')
				cur.execute("SELECT * FROM files WHERE individual in (" +",".join(["?"]*len(inds))+")" + " limit "+limit, inds)

			conn.commit()

			rows = cur.fetchall()

			if(len(rows) == 0):
				logger.info('No files chosen.')
				self.redirect("/launch")
			else:
				logger.info("Number of files chosen = " + str(len(rows)) + ":")

			for row in rows:
				logger.info("------- %s " % (str(row[2])))
				bam_files.append(row[2])
				selected_individuals.append(row[3])


		#remove the filter arguments that specify which files to choose
		query_args.pop("subpops", None)
		query_args.pop("inds", None)

		if "regions" in query_args and "api_key" in query_args:
			regions_split = query_args["regions"][0].split(",")
			query_args["regions"] = regions_split

		if not SAMFilter.check_validity(query_args):
			self.write('Not a valid query.')
			return


		job = group([readBAM.s(bamfile,query_args) for bamfile in bam_files])
		a_result = job.apply_async(exchange='default', routing_key='default')


		today = datetime.today()
		now = today.strftime("%Y-%m-%d %H:%M:%S")

		#create records in the history file
		cur = conn.cursor()

		# use files table as temporary placeholder - change tracking to current tasks's tracking for the selected files
		cur.execute("UPDATE files SET tracking='" + a_result.id +"',rundate='" + now + "',status='DISPATCHED' WHERE individual in (%s)" %
							  ','.join('?'*len(selected_individuals)), selected_individuals)

		# only insert the records with this tasks tracking into fileshist
		cur.execute("INSERT into fileshist select * from files WHERE tracking=?", (a_result.id,))

		conn.commit()
		conn.close()

		logger.info("Started filtering job of group " +  a_result.id)

		if "api_key" in query_args:
			self.write(a_result.id)
		else:
			self.redirect("/dash?tracking="+a_result.id)


def main():
	logger.info("DFAAS starting" )
	tornado.options.parse_command_line()
	http_server = tornado.httpserver.HTTPServer(Application())
	http_server.listen(8888)
	logger.info("DFAAS ready" )
	tornado.ioloop.IOLoop.current().start()

if __name__ == "__main__":
	main()

