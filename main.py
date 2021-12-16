'''Copyright 2018 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.'''

import json
from flask import Flask, request
import datetime
import logging
import google.appengine.api 
from google.appengine.api import urlfetch
import urllib
import uuid
from google.appengine.ext import ndb
from google.cloud import storage
from google.cloud import bigquery
import unidecode

app = Flask(__name__)
app.wsgi_app = google.appengine.api.wrap_wsgi_app(app.wsgi_app)

# Your Waze CCP URL
wazeURL= '{waze-url}'

""" **** Remove this line and the quotes here and at the bottom of this block if using Carto ***
cartoURLBase = 'https://{your-carto-server}/user/{your-user}/api/v2/sql?'
cartoAPIKey = '{your-carto-api-key}'
"""

#GCS Params
bucket_name = '{gcsBucket}'
gcs = storage.Client()
BUCKET = gcs.get_bucket(bucket_name)
gcsPath = '/'+ bucket_name +'/'

#BigQuery Params
bqDataset = '{bqDataset}'

#BigQuery Schemas for the three tables that need to be recreated.
#These are also referenced with each write.

jamsSchema = [
 			bigquery.SchemaField('city','STRING',mode='Nullable'),
 			bigquery.SchemaField('turntype','STRING',mode='Nullable'),
 			bigquery.SchemaField('level','INT64',mode='Nullable'),
 			bigquery.SchemaField('country','STRING',mode='Nullable'),
 			bigquery.SchemaField('speedKMH','FLOAT64',mode='Nullable'),
 			bigquery.SchemaField('delay','INT64',mode='Nullable'),
 			bigquery.SchemaField('length','INT64',mode='Nullable'),
 			bigquery.SchemaField('street','STRING',mode='Nullable'),
 			bigquery.SchemaField('ms','INT64',mode='Nullable'),
 			bigquery.SchemaField('ts','TIMESTAMP',mode='Nullable'),
 			bigquery.SchemaField('endNode','STRING',mode='Nullable'),
 			bigquery.SchemaField('geo','GEOGRAPHY',mode='Nullable'),
 			bigquery.SchemaField('geoWKT','STRING',mode='Nullable'),
 			bigquery.SchemaField('type','STRING',mode='Nullable'),
 			bigquery.SchemaField('id','INT64',mode='Nullable'),
 			bigquery.SchemaField('speed','FLOAT64',mode='Nullable'),
 			bigquery.SchemaField('uuid','STRING',mode='Nullable'),
 			bigquery.SchemaField('startNode','STRING',mode='Nullable'),
 		]


alertsSchema = [
 			bigquery.SchemaField('city','STRING',mode='Nullable'),
 			bigquery.SchemaField('confidence','INT64',mode='Nullable'),
 			bigquery.SchemaField('nThumbsUp','INT64',mode='Nullable'),
 			bigquery.SchemaField('street','STRING',mode='Nullable'),
 			bigquery.SchemaField('uuid','STRING',mode='Nullable'),
 			bigquery.SchemaField('country','STRING',mode='Nullable'),
 			bigquery.SchemaField('type','STRING',mode='Nullable'),
 			bigquery.SchemaField('subtype','STRING',mode='Nullable'),
 			bigquery.SchemaField('roadType','INT64',mode='Nullable'),
 			bigquery.SchemaField('reliability','INT64',mode='Nullable'),
 			bigquery.SchemaField('magvar','INT64',mode='Nullable'),
 			bigquery.SchemaField('reportRating','INT64',mode='Nullable'),
 			bigquery.SchemaField('ms','INT64',mode='Nullable'),
 			bigquery.SchemaField('ts','TIMESTAMP',mode='Nullable'),
 			bigquery.SchemaField('reportDescription','STRING',mode='Nullable'),
 			bigquery.SchemaField('geo','GEOGRAPHY',mode='Nullable'),
 			bigquery.SchemaField('geoWKT','STRING',mode='Nullable')
 		]
irregularitiesSchema =[
 			bigquery.SchemaField('trend','INT64',mode='Nullable'),
 			bigquery.SchemaField('street','STRING',mode='Nullable'),
 			bigquery.SchemaField('endNode','STRING',mode='Nullable'),
 			bigquery.SchemaField('nImages','INT64',mode='Nullable'),
 			bigquery.SchemaField('speed','FLOAT64',mode='Nullable'),
 			bigquery.SchemaField('id','STRING',mode='Nullable'),
 			bigquery.SchemaField('severity','INT64',mode='Nullable'),
 			bigquery.SchemaField('type','STRING',mode='Nullable'),
 			bigquery.SchemaField('highway','BOOL',mode='Nullable'),
 			bigquery.SchemaField('nThumbsUp','INT64',mode='Nullable'),
 			bigquery.SchemaField('seconds','INT64',mode='Nullable'),
 			bigquery.SchemaField('alertsCount','INT64',mode='Nullable'),
 			bigquery.SchemaField('detectionDateMS','INT64',mode='Nullable'),
 			bigquery.SchemaField('detectionDateTS','TIMESTAMP',mode='Nullable'),
 			bigquery.SchemaField('driversCount','INT64',mode='Nullable'),
 			bigquery.SchemaField('geo','GEOGRAPHY',mode='Nullable'),
 			bigquery.SchemaField('geoWKT','STRING',mode='Nullable'),
 			bigquery.SchemaField('startNode','STRING',mode='Nullable'),
 			bigquery.SchemaField('updateDateMS','INT64',mode='Nullable'),
 			bigquery.SchemaField('updateDateTS','TIMESTAMP',mode='Nullable'),
 			bigquery.SchemaField('regularSpeed','FLOAT64',mode='Nullable'),
 			bigquery.SchemaField('country','STRING',mode='Nullable'),
 			bigquery.SchemaField('length','INT64',mode='Nullable'),
 			bigquery.SchemaField('delaySeconds','INT64',mode='Nullable'),
 			bigquery.SchemaField('jamLevel','INT64',mode='Nullable'),
 			bigquery.SchemaField('nComments','INT64',mode='Nullable'),
 			bigquery.SchemaField('city','STRING',mode='Nullable'),
 			bigquery.SchemaField('causeType','STRING',mode='Nullable'),
 			bigquery.SchemaField('causeAlertUUID','STRING',mode='Nullable'),

 		]

""" **** Remove this line and the quotes here and at the bottom of this block if using Carto ***

#cartoSQL Scehmas and Values Strings

cartoAlertsSchema = "(city text,\
	confidence int,\
	nThumbsUp int,\
	street text,\
	uuid text,\
	country text,\
	type text,\
	subtype text,\
	roadType int,\
	reliability int,\
	magvar int,\
	reportRating int,\
	ms bigint,\
	ts timestamp,\
	reportDescription text,\
	the_geom geometry\
)"

cartoAlertsFields = "(city,confidence,nThumbsUp,street,uuid,country,type,subtype,roadType,reliability,magvar,reportRating,ms,ts,reportDescription,the_geom)"

cartoJamsScehma = "(city text,\
	turntype text,\
	level int,\
	country text,\
	speedKMH real,\
	delay int,\
	length int,\
	street text,\
	ms bigint,\
	ts timestamp,\
	endNode text,\
	the_geom geometry,\
	type text,\
	id bigint,\
	speed real,\
	uuid text,\
	startNode text\
)"

cartoJamsFields = "(city,turntype,level,country,speedKMH,delay,length,street,ms,ts,endNode,type,id,speed,uuid,startNode,the_geom)"

cartoIrregularitiesScehma = "(trend int,\
	street text,\
	endNode text,\
	nImages int,\
	speed real,\
	id text,\
	severity int,\
	type text,\
	highway bool,\
	nThumbsUp int,\
	seconds int,\
	alertsCount int,\
	detectionDateMS bigint,\
	detectionDateTS timestamp,\
	driversCount int,\
	the_geom geometry,\
	startNode text,\
	updateDateMS bigint,\
	updateDateTS timestamp,\
	regularSpeed real,\
	country text,\
	length int,\
	delaySeconds int,\
	jamLevel int,\
	nComments int,\
	city text,\
	causeType text,\
	causeAlertUUID text\
)"

cartoIrregularitiesFields = "(trend,street,endNode,nImages,speed,id,severity,type,highway,nThumbsUp,seconds,alertsCount,detectionDateMS,detectionDateTS,driversCount,startNode,updateDateMS,updateDateTS,regularSpeed,country,length,delaySeconds,jamLevel,nComments,city,causeType,causeAlertUUID,the_geom)"
"""
#Define a Datastore ndb Model for each Waze "case" (Study area) that you want to monitor
class caseModel(ndb.Model):
  uid = ndb.StringProperty()
  name = ndb.StringProperty()
  day = ndb.StringProperty()

#This application will track unique entities for Jams, Alerts, and Irregularities.
#We don't want to write duplicate events to BigQuery if they persist through the refresh window.

#Define an ndb Model to track unique Jams.
class uniqueJams(ndb.Model):
	tableUUID = ndb.StringProperty()
	jamsUUID = ndb.StringProperty()

#Define an ndb Model to track unique Alerts.
class uniqueAlerts(ndb.Model):
	tableUUID = ndb.StringProperty()
	alertsUUID = ndb.StringProperty()

#Define an ndb Model to track unique Irregularities.
class uniqueIrregularities(ndb.Model):
	tableUUID = ndb.StringProperty()
	irregularitiesUUID = ndb.StringProperty()

#App Request Handler to create a new Case.
#Called ONCE as: {your-app}.appspot.com/newCase/?name={your-case-name}
#This handler can be disabled after you create your first case if you
#only intend to create one.
@app.route("/newCase/", methods=['GET'])
def newCase():
	uid = uuid.uuid4()
	name = request.args.get("name")
	day = datetime.datetime.now().strftime("%Y-%m-%d")
	if not name:
		name = ""

	#Write the new Case details to Datastore
	wazePut = caseModel(uid=str(uid),day=day,name=name)
	wazeKey = wazePut.put()

	#Create the BigQuery Client
	client = bigquery.Client()
	datasetRef = client.dataset(bqDataset)
	tableSuffix = str(uid).replace('-','_')

	#Create the Jams Table
	jamsTable = 'jams_' + tableSuffix
	tableRef = datasetRef.table(jamsTable)
	table = bigquery.Table(tableRef,schema=jamsSchema)
	table = client.create_table(table)
	assert table.table_id == jamsTable


	#Create the Alerts Table
	alertsTable = 'alerts_' + tableSuffix
	tableRef = datasetRef.table(alertsTable)
	table = bigquery.Table(tableRef,schema=alertsSchema)
	table = client.create_table(table)
	assert table.table_id == alertsTable


	#Create the Irregularities Table
	irregularitiesTable = 'irregularities_' + tableSuffix
	tableRef = datasetRef.table(irregularitiesTable)
	table = bigquery.Table(tableRef,schema=irregularitiesSchema)
	table = client.create_table(table)
	assert table.table_id == irregularitiesTable

""" **** Remove this line and the quotes here and at the bottom of this block to also register the Case Study to Carto using CartoSQL ***

 		# Create and register Alerts Table in Carto

	 	url = cartoURLBase + "q=CREATE TABLE " + alertsTable + " " + cartoAlertsSchema + "&api_key=" + cartoAPIKey
		logging.info(url)
		try:
			result = urlfetch.fetch(url.replace(" ", "%20"), validate_certificate=True)
			if result.status_code == 200:
				url = cartoURLBase + "q=SELECT cdb_cartodbfytable('" + alertsTable + "')&api_key=" + cartoAPIKey
				logging.info(url)
				try:
					result = urlfetch.fetch(url.replace(" ", "%20"), validate_certificate=True)
					if result.status_code == 200:
						logging.info('Created and Cartodbfyd Table ' + alertsTable)
				except urlfetch.Error:
					logging.exception('Caught exception Cartodbfying Waze Alerts Table ' + alertsTable)
			else:
				logging.exception(result.status_code)
		except urlfetch.Error:
			logging.exception('Caught exception Creating Waze Alerts Table ' + alertsTable)

		# Create and register Jams Table in Carto

	 	url = cartoURLBase + "q=CREATE TABLE " + jamsTable + " " + cartoJamsScehma + "&api_key=" + cartoAPIKey
		logging.info(url)
		try:
			result = urlfetch.fetch(url.replace(" ", "%20"), validate_certificate=True)
			if result.status_code == 200:
				url = cartoURLBase + "q=SELECT cdb_cartodbfytable('" + jamsTable + "')&api_key=" + cartoAPIKey
				logging.info(url)
				try:
					result = urlfetch.fetch(url.replace(" ", "%20"), validate_certificate=True)
					if result.status_code == 200:
						logging.info('Created and Cartodbfyd Table ' + jamsTable)
				except urlfetch.Error:
					logging.exception('Caught exception Cartodbfying Waze Jams Table ' + jamsTable)
			else:
				logging.exception(result.status_code)
		except urlfetch.Error:
			logging.exception('Caught exception Creating Waze Jams Table ' + jamsTable)

		# Create and register Irregularities Table in Carto

	 	url = cartoURLBase + "q=CREATE TABLE " + irregularitiesTable + " " + cartoIrregularitiesScehma + "&api_key=" + cartoAPIKey
		logging.info(url)
		try:
			result = urlfetch.fetch(url.replace(" ", "%20"), validate_certificate=True)
			if result.status_code == 200:
				url = cartoURLBase + "q=SELECT cdb_cartodbfytable('" + irregularitiesTable + "')&api_key=" + cartoAPIKey
				logging.info(url)
				try:
					result = urlfetch.fetch(url.replace(" ", "%20"), validate_certificate=True)
					if result.status_code == 200:
						logging.info('Created and Cartodbfyd Table ' + irregularitiesTable)
				except urlfetch.Error:
					logging.exception('Caught exception Cartodbfying Waze Jams Table ' + irregularitiesTable)
			else:
				logging.exception(result.status_code)
		except urlfetch.Error:
			logging.exception('Caught exception Creating Waze Jams Table ' + irregularitiesTable)

"""

#Called at your set cron interval, this function loops through all the cases in datastore
#And adds a task to update the case's tables in Taskqeue
@app.route('/{guid}/', methods=['GET'])
def updateCaseStudies():
	caseStudies = caseModel.query()
	for case in caseStudies:
		updateCase(case)
	return 'OK'

#For each Case, update each table
def updateCase(case):
	url = wazeURL
	try:
		result = urlfetch.fetch(url)
		if result.status_code == 200:
			data = json.loads(result.content)
			#Get the 3 components from the Waze CCP JSON Response
			alerts = data.get('alerts')
			jams = data.get('jams')
			irregularities = data.get('irregularities')
			if alerts is not None:
				processAlerts(data['alerts'],case.uid,case.day)
			if jams is not None:
				processJams(data['jams'],case.uid,case.day)
			if irregularities is not None:
				processIrregularities(data['irregularities'],case.uid,case.day)
		else:
			logging.exception(result.status_code)
	except urlfetch.Error:
		logging.exception('Caught exception fetching Waze URL')

#Process the Alerts
def processAlerts(alerts,uid,day):
	now = datetime.datetime.now().strftime("%s")
	features = []
	bqRows = []
	cartoRows =[]
	for item in alerts:
		ms = item.get('pubMillis')
		itemTimeStamp = datetime.datetime.fromtimestamp(ms/1000.0)
		caseTimeMinimum = datetime.datetime.strptime(day, "%Y-%m-%d")
		if itemTimeStamp >= caseTimeMinimum:
			timestamp = datetime.datetime.fromtimestamp(ms/1000.0).strftime("%Y-%m-%d %H:%M:%S")
			city = item.get('city')
			street = item.get('street')
			confidence = item.get('confidence')
			nThumbsUp = item.get('nThumbsUp')
			uuid = item.get('uuid')
			country = item.get('country')
			subtype = item.get('subtype')
			roadType = item.get('roadType')
			reliability = item.get('reliability')
			magvar = item.get('magvar')
			alertType = item.get('type')
			reportRating = item.get('reportRating')
			reportDescription = item.get('reportDescription')
			longitude = item.get('location').get('x')
			latitude = item.get('location').get('y')
			#Create GCS GeoJSON Properties
			properties = {"city": city,
				"street": street,
				"confidence": confidence,
				"nThumbsUp": nThumbsUp,
				"uuid": uuid,
				"country": country,
				"subtype": subtype,
				"roadType": roadType,
				"reliability": reliability,
				"magvar": magvar,
				"type": alertType,
				"reportRating": reportRating,
				"pubMillis": ms,
				"timestamp": timestamp,
				"reportDescription": reportDescription,
			}
			geometry = {"type": "Point",
							"coordinates": [longitude, latitude]
			}
			features.append({"type": "Feature", "properties": properties, "geometry": geometry})
			#BigQuery Row Creation
			datastoreQuery = uniqueAlerts.query(uniqueAlerts.tableUUID == uid, uniqueAlerts.alertsUUID == uuid)
			datastoreCheck = datastoreQuery.get()
			if not datastoreCheck:
				bqRow = {"city": city,
							"street": street,
							"confidence": confidence,
							"nThumbsUp": nThumbsUp,
							"uuid": uuid,
							"country": country,
							"subtype": subtype,
							"roadType": roadType,
							"reliability": reliability,
							"magvar": magvar,
							"type": alertType,
							"reportRating": reportRating,
							"ms": ms,
							"ts": timestamp,
							"reportDescription": reportDescription,
							"geo": "Point(" + str(longitude) + " " + str(latitude) + ")",
							"geoWKT": "Point(" + str(longitude) + " " + str(latitude) + ")"
				}
				bqRows.append(bqRow)
				""" **** Remove this line and the quotes here and at the bottom of this block to also use Carto ***

				# Create Carto Row
				if city is not None:
					city = unidecode.unidecode(city)
				if street is not None:
					street = unidecode.unidecode(street)
				if uuid is not None:
					uuid = unidecode.unidecode(uuid)
				if country is not None:
					country = unidecode.unidecode(country)
				if alertType is not None:
					alertType = unidecode.unidecode(alertType)
				if subtype is not None:
					subtype = unidecode.unidecode(subtype)
				if reportDescription is not None:
					reportDescription = unidecode.unidecode(reportDescription)

				cartoRow = "('{city}',{confidence},{nThumbsUp},'{street}','{uuid}','{country}','{type}','{subtype}',{roadType},{reliability},{magvar},{reportRating},{ms},'{ts}','{reportDescription}',ST_GeomFromText({the_geom},4326))".format(
					city=city, confidence=confidence, nThumbsUp=nThumbsUp, street=street, uuid=uuid, country=country,
					type=alertType, subtype=subtype, roadType=roadType, reliability=reliability, magvar=magvar,
					reportRating=reportRating, ms=ms, ts=timestamp, reportDescription=reportDescription, the_geom="'Point(" + str(longitude) + " " + str(latitude) + ")'")
				cartoRows.append(cartoRow)
				"""

				#Add uuid to Datastore
				alertPut = uniqueAlerts(tableUUID=str(uid), alertsUUID=str(uuid))
				alertKey = alertPut.put()

 	#Write GeoJSONs to GCS
	alertGeoJSON = json.dumps({"type": "FeatureCollection", "features": features})
	writeGeoJSON(alertGeoJSON, uid + '/' + uid + '-alerts.geojson')
	writeGeoJSON(alertGeoJSON, uid + '/' + uid + '-' + now +'-alerts.geojson')

	alertsTable = 'alerts_' + str(uid).replace('-','_')
	#Stream new Rows to BigQuery
	if bqRows:
		client = bigquery.Client()
		datasetRef = client.dataset(bqDataset)
		tableRef = datasetRef.table(alertsTable)
		table = bigquery.Table(tableRef,schema=alertsSchema)
		errors = client.insert_rows(table, bqRows)
		try:
			assert errors == []
			logging.info(errors)
		except AssertionError as e:
			logging.warning(e)
	""" **** Remove this line and the quotes here and at the bottom of this block to also use Carto ***
	# Load Rows to Carto

	if cartoRows:
		cartoQuery = "INSERT INTO " + alertsTable + " " + cartoAlertsFields + "VALUES " + ",".join(cartoRows)
		cartoQuery = cartoQuery.replace("'None'","null").replace("None","null")
		cartoQueryEncoded = urllib.urlencode({"q":cartoQuery})
		# logging.info(cartoQuery)
		# logging.info(cartoQueryEncoded)


		# url = cartoURLBase + cartoQueryEncoded + "&api_key=" + cartoAPIKey
		url = cartoURLBase + "&api_key=" + cartoAPIKey
		# logging.info(url)
		try:
			result = urlfetch.fetch(url, validate_certificate=True,method=urlfetch.POST,payload=cartoQueryEncoded)
			if result.status_code == 200:
				logging.info('Inserted Alerts Data to Carto')
			else:
				logging.exception(result.status_code)
				logging.exception(cartoQuery)
				writeSQLError(cartoQuery,gcsPath + 'carto_errors/' + uid + '-' + now + '-alerts.txt')
		except urlfetch.Error:
			logging.exception('Caught exception Inserting Data to Alerts Table ' + alertsTable)
	"""
#Process the Jams
def processJams(jams,uid,day):
	now = datetime.datetime.now().strftime("%s")
	features = []
	bqRows =[]
	cartoRows = []
	for item in jams:
		ms = item.get('pubMillis')
		itemTimeStamp = datetime.datetime.fromtimestamp(ms/1000.0)
		caseTimeMinimum = datetime.datetime.strptime(day, "%Y-%m-%d")
		if itemTimeStamp >= caseTimeMinimum:
			timestamp = datetime.datetime.fromtimestamp(ms/1000.0).strftime("%Y-%m-%d %H:%M:%S")
			city = item.get('city')
			turnType = item.get('turnType')
			level = item.get('level')
			country = item.get('country')
			segments = item.get('segments')
			speedKMH = item.get('speedKMH')
			roadType = item.get('roadType')
			delay = item.get('delay')
			length = item.get('length')
			street = item.get('street')
			endNode = item.get('endNode')
			jamType = item.get('type')
			iD = item.get('id')
			uuid = item.get('uuid')
			speed = item.get('speed')
			startNode = item.get('startNode')
			#Create GCS GeoJSON Properties
			properties = {"city": city,
				"turnType": turnType,
				"level": level,
				"country": country,
				"segments": segments,
				"speedKMH": speedKMH,
				"roadType": roadType,
				"delay": delay,
				"length": length,
				"street": street,
				"pubMillis": ms,
				"timestamp": timestamp,
				"endNode": endNode,
				"type": jamType,
				"id": iD,
				"speed": speed,
				"uuid": uuid,
				"startNode": startNode
			}
			#Create WKT Polyline for BigQuery
			coordinates = []
			bqLineString = ''
			for vertex in item.get('line'):
				longitude = vertex.get('x')
				latitude = vertex.get('y')
				coordinate = [longitude, latitude]
				bqLineString += str(longitude) + " " + str(latitude) + ', '
				coordinates.append(coordinate)
			geometry = {"type": "LineString",
							"coordinates": coordinates
			}
			features.append({"type": "Feature", "properties": properties, "geometry": geometry})
			#BigQuery Row Creation
			datastoreQuery = uniqueJams.query(uniqueJams.tableUUID == str(uid), uniqueJams.jamsUUID == str(uuid))
			datastoreCheck = datastoreQuery.get()
			if not datastoreCheck:
				bqRow = {"city": city,
							"turnType": turnType,
							"level": level,
							"country": country,
							"segments": segments,
							"speedKMH": speedKMH,
							"roadType": roadType,
							"delay": delay,
							"length": length,
							"street": street,
							"ms": ms,
							"ts": timestamp,
							"endNode": endNode,
							"type": jamType,
							"id": iD,
							"speed": speed,
							"uuid": uuid,
							"startNode": startNode,
							"geo": "LineString(" + bqLineString[:-2] + ")",
							"geoWKT": "LineString(" + bqLineString[:-2] + ")"
				}
				bqRows.append(bqRow)
				"""" **** Remove this line and the quotes here and at the bottom of this block to also use Carto ***
				# Create Carto Row

				if city is not None:
					city = unidecode.unidecode(city)
				if turnType is not None:
					turnType = unidecode.unidecode(turnType)
				if country is not None:
					country = unidecode.unidecode(country)
				if street is not None:
					street = unidecode.unidecode(street)
				if endNode is not None:
					endNode = unidecode.unidecode(endNode)
				if jamType is not None:
					jamType = unidecode.unidecode(jamType)
				if startNode is not None:
					startNode = unidecode.unidecode(startNode)

				cartoRow = "('{city}','{turntype}',{level},'{country}',{speedKMH},{delay},{length},'{street}',{ms},'{ts}','{endNode}','{type}',{id},{speed},'{uuid}','{startNode}',ST_GeomFromText({the_geom},4326))".format(
					city=city, turntype=turnType, level=level, country=country, speedKMH=speedKMH, delay=delay,
					length=length, street=street, ms=ms, ts=timestamp, endNode=endNode,
					type=jamType, id=iD, speed=speed, uuid=uuid, startNode=startNode, the_geom="'LineString(" + bqLineString[:-2] + ")'")
				cartoRows.append(cartoRow)
				"""

				#Add uuid to Datastore
				jamPut = uniqueJams(tableUUID=str(uid), jamsUUID=str(uuid))
				jamKey = jamPut.put()
	jamsGeoJSON = json.dumps({"type": "FeatureCollection", "features": features})
	writeGeoJSON(jamsGeoJSON, uid + '/' + uid + '-jams.geojson')
	writeGeoJSON(jamsGeoJSON, uid + '/' + uid + '-' + now +'-jams.geojson')

	#Stream new Rows to BigQuery
	if bqRows:
		client = bigquery.Client()
		datasetRef = client.dataset(bqDataset)
		jamsTable = 'jams_' + str(uid).replace('-','_')
		tableRef = datasetRef.table(jamsTable)
		table = bigquery.Table(tableRef,schema=jamsSchema)
		errors = client.insert_rows(table, bqRows)
		try:
			assert errors == []
			logging.info(errors)
		except AssertionError as e:
			logging.warning(e)
	""" **** Remove this line and the quotes here and at the bottom of this block to also use Carto ***
	# Load Rows to Carto

	if cartoRows:
		cartoQuery = "INSERT INTO " + jamsTable + " " + cartoJamsFields + "VALUES " + ",".join(cartoRows)
		cartoQuery = cartoQuery.replace("'None'","null").replace("None","null")
		cartoQueryEncoded = urllib.urlencode({"q":cartoQuery})
		logging.info(len(cartoQueryEncoded))
		# logging.info(cartoQuery)
		# logging.info(cartoQueryEncoded)


		url = cartoURLBase + "&api_key=" + cartoAPIKey
		logging.info(url)
		try:
			result = urlfetch.fetch(url, validate_certificate=True,method=urlfetch.POST,payload=cartoQueryEncoded)
			if result.status_code == 200:
				logging.info('Inserted Jams Data to Carto')
			else:
				logging.exception(result.status_code)
				logging.exception(cartoQuery)
				writeSQLError(cartoQuery,gcsPath + 'carto_errors/' + uid + '-' + now + '-jams.txt')
		except urlfetch.Error:
			logging.exception('Caught exception Inserting Data to Jams Table ' + jamsTable)
	"""

#Process the Irregularities
def processIrregularities(irregularities,uid,day):
	now = datetime.datetime.now().strftime("%s")
	features = []
	bqRows = []
	cartoRows = []

	for item in irregularities:
		detectionDateMS = item.get('detectionDateMillis')
		itemTimeStamp = datetime.datetime.fromtimestamp(detectionDateMS/1000.0)
		caseTimeMinimum = datetime.datetime.strptime(day, "%Y-%m-%d")
		if itemTimeStamp >= caseTimeMinimum:
			detectionDateTS = datetime.datetime.fromtimestamp(detectionDateMS/1000.0).strftime("%Y-%m-%d %H:%M:%S")
			updateDateMS = item.get('updateDateMillis')
			updateDateTS = datetime.datetime.fromtimestamp(updateDateMS/1000.0).strftime("%Y-%m-%d %H:%M:%S")
			causeAlert = item.get('causeAlert')
			if causeAlert is not None:
				causeAlertUUID = causeAlert.get('uuid')
			else:
				causeAlertUUID = None
			trend = item.get('trend')
			street = item.get('street')
			endNode =  item.get('endNode')
			nImages = item.get('nImages')
			speed = item.get('speed')
			iD = item.get('id')
			severity = item.get('severity')
			irregularityType = item.get('type')
			highway = item.get('highway')
			nThumbsUp = item.get('nThumbsUp')
			seconds = item.get('seconds')
			alertsCount = item.get('alertsCount')
			driversCount = item.get('driversCount')
			startNode = item.get('startNode')
			regularSpeed = item.get('regularSpeed')
			country = item.get('country')
			length = item.get('length')
			delaySeconds = item.get('delaySeconds')
			jamLevel = item.get('jamLevel')
			nComments = item.get('nComments')
			city = item.get('city')
			causeType = item.get('causeType')
			#Create GCS GeoJSON Properties
			properties = {"trend": trend,
				"street": street,
				"endNode": endNode,
				"nImages": nImages,
				"speed": speed,
				"id": iD,
				"severity": severity,
				"type": irregularityType,
				"highway": highway,
				"nThumbsUp": nThumbsUp,
				"seconds": seconds ,
				"alertsCount": alertsCount,
				"driversCount": driversCount ,
				"startNode": startNode,
				"regularSpeed": regularSpeed,
				"country": country,
				"length": length,
				"delaySeconds": delaySeconds,
				"jamLevel": jamLevel ,
				"nComments": nComments,
				"city": city,
				"causeType": causeType,
				"detectionDateMS": detectionDateMS,
				"detectionDateTS": detectionDateTS,
				"updateDateMS": updateDateMS,
				"updateDateTS": updateDateTS,
				"causeAlertUUID": causeAlertUUID
			}
			#Create WKT Polyline for BigQuery
			coordinates = []
			bqLineString = ''
			for vertex in item.get('line'):
				longitude = vertex.get('x')
				latitude = vertex.get('y')
				coordinate = [longitude, latitude]
				bqLineString += str(longitude) + " " + str(latitude) + ', '
				coordinates.append(coordinate)
			geometry = {"type": "LineString",
							"coordinates": coordinates
			}
			features.append({"type": "Feature", "properties": properties, "geometry": geometry})

			#BigQuery Row Creation
			datastoreQuery = uniqueIrregularities.query(uniqueIrregularities.tableUUID == str(uid), uniqueIrregularities.irregularitiesUUID == str(iD)+str(updateDateMS))
			datastoreCheck = datastoreQuery.get()
			if not datastoreCheck:
				bqRow = {"trend": trend,
							"street": street,
							"endNode": endNode,
							"nImages": nImages,
							"speed": speed,
							"id": iD,
							"severity": severity,
							"type": irregularityType,
							"highway": highway,
							"nThumbsUp": nThumbsUp,
							"seconds": seconds ,
							"alertsCount": alertsCount,
							"driversCount": driversCount ,
							"startNode": startNode,
							"regularSpeed": regularSpeed,
							"country": country,
							"length": length,
							"delaySeconds": delaySeconds,
							"jamLevel": jamLevel ,
							"nComments": nComments,
							"city": city,
							"causeType": causeType,
							"detectionDateMS": detectionDateMS,
							"detectionDateTS": detectionDateTS,
							"updateDateMS": updateDateMS,
							"updateDateTS": updateDateTS,
							"causeAlertUUID": causeAlertUUID,
							"geo": "LineString(" + bqLineString[:-2] + ")",
							"geoWKT": "LineString(" + bqLineString[:-2] + ")"
				}
				bqRows.append(bqRow)
				""" **** Remove this line and the quotes here and at the bottom of this block to also use Carto ***
				# Create Carto Row

				if street is not None:
					street = unidecode.unidecode(street)
				if endNode is not None:
					endNode = unidecode.unidecode(endNode)
				if irregularityType is not None:
					irregularityType = unidecode.unidecode(irregularityType)
				if startNode is not None:
					startNode = unidecode.unidecode(startNode)
				if city is not None:
					city = unidecode.unidecode(city)
				if causeType is not None:
					causeType = unidecode.unidecode(causeType)
				if causeAlertUUID is not None:
					causeAlertUUID = unidecode.unidecode(causeAlertUUID)

				cartoRow = "({trend},'{street}','{endNode}',{nImages},{speed},'{id}',{severity},'{type}',{highway},{nThumbsUp},{seconds},{alertsCount},{detectionDateMS},'{detectionDateTS}',{driversCount},'{startNode}',{updateDateMS},'{updateDateTS}',{regularSpeed},'{country}',{length},{delaySeconds},{jamLevel},{nComments},'{city}','{causeType}','{causeAlertUUID}',ST_GeomFromText({the_geom},4326))".format(
					trend=trend, street=street, endNode=endNode, nImages=nImages,speed=speed,id=iD,severity=severity,type=irregularityType,
					highway=highway, nThumbsUp=nThumbsUp,seconds=seconds,alertsCount=alertsCount,detectionDateMS=detectionDateMS,detectionDateTS=detectionDateTS,
					driversCount=driversCount,startNode=startNode,updateDateMS=updateDateMS, updateDateTS=updateDateTS, regularSpeed=regularSpeed, country=country,length=length,
					delaySeconds=delaySeconds, jamLevel=jamLevel,nComments=nComments, city=city, causeType=causeType, causeAlertUUID=causeAlertUUID, the_geom="'LineString(" + bqLineString[:-2] + ")'")
				cartoRows.append(cartoRow)
				"""

				#Add uuid to Datastore
				irregularityPut = uniqueIrregularities(tableUUID=str(uid), irregularitiesUUID=str(iD)+str(updateDateMS))
				irregularityKey = irregularityPut.put()
	irregularitiesGeoJSON = json.dumps({"type": "FeatureCollection", "features": features})
	writeGeoJSON(irregularitiesGeoJSON, uid + '/' + uid + '-irregularities.geojson')
	writeGeoJSON(irregularitiesGeoJSON, uid + '/' + uid + '-' + now +'-irregularities.geojson')
	# logging.info(irregularitiesGeoJSON)

	#Stream new Rows to BigQuery
	if bqRows:
		client = bigquery.Client()
		datasetRef = client.dataset(bqDataset)
		irregularitiesTable = 'irregularities_' + str(uid).replace('-','_')
		tableRef = datasetRef.table(irregularitiesTable)
		table = bigquery.Table(tableRef,schema=irregularitiesSchema)
		errors = client.insert_rows(table, bqRows)
		try:
			assert errors == []
			logging.info(errors)
		except AssertionError as e:
			logging.warning(e)

	""" **** Remove this line and the quotes here and at the bottom of this block to also use Carto ***
	# Load Rows to Carto

	if cartoRows:
		cartoQuery = "INSERT INTO " + irregularitiesTable + " " + cartoIrregularitiesFields + "VALUES " + ",".join(cartoRows)
		cartoQuery = cartoQuery.replace("'None'","null").replace("None","null")
		cartoQueryEncoded = urllib.urlencode({"q":cartoQuery})
		# logging.info(cartoQuery)
		# logging.info(cartoQueryEncoded)


		url = cartoURLBase  + "&api_key=" + cartoAPIKey
		# logging.info(url)
		try:
			result = urlfetch.fetch(url, validate_certificate=True,method=urlfetch.POST,payload=cartoQueryEncoded)
			if result.status_code == 200:
				logging.info('Inserted Irregularities Data to Carto')
			else:
				logging.exception(result.status_code)
				logging.exception(cartoQuery)
				writeSQLError(cartoQuery,gcsPath + 'carto_errors/' + uid + '-' + now + '-irregularities.txt')
		except urlfetch.Error:
			logging.exception('Caught exception Inserting Data to Irregularities Table ' + irregularitiesTable)
	"""
#Write the GeoJSON to GCS
def writeGeoJSON(geoJSON,filename):
	blob = BUCKET.blob(filename)
	blob.upload_from_string(
        data=json.dumps(geoJSON),
        content_type='application/json'
        )
	result = filename + ' upload complete'
	return {'response' : result}

""" **** Remove this line and the quotes here and at the bottom of this block to also use Carto ***
def writeSQLError(sql,filename):
	blob = BUCKET.blob(filename)
	blob.upload_from_string(
        data=sql,
        content_type='text/html'
        )
"""