import os
import re
import sys
import json
import gzip
import luigi
import pdb
from datetime import date, datetime, timedelta
from luigi.contrib import bigquery, gcs
import boto3
import botocore
from oauth2client.client import GoogleCredentials
from xx_utils import check_partition_modified

import logging
logger = logging.getLogger("etl")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - ' + os.path.basename(__file__) + ' - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)





class process_raw_snowplow_event_data(luigi.Task):
	dataset_date = luigi.DateParameter(default=date.today() - timedelta(days=1))
	# force_run = luigi.BoolParameter()
	_start = luigi.DateSecondParameter(default=datetime.utcnow())
	file_root = luigi.Parameter()
	sp_bucket='xxx'
	s3 = boto3.client('s3', 'us-west-2',
						aws_access_key_id=os.environ.get('AWS_ACCESS'),
						aws_secret_access_key=os.environ.get('AWS_SECRET')
						)


	def download_s3_file(self, s3_filename):

		local_filename = "/etl/%s" % s3_filename
	
		s3_file_full_path = re.compile(r"xx/enriched/archive/run=" + self.dataset_date.strftime("%Y-%m-%d") +r"-\d{2}-\d{2}-\d{2}/")

		try:
			s3.download_file(Bucket='xx', Key=s3_file_full_path, Filename=local_filename)
		except Exception as e:
			logger.error("%s - Could not retrieve %s because: %s" % ("download_s3_file()", s3_filename, e))
			raise

		return local_filename	

	def output(self):
		return luigi.LocalTarget("/etl/%s_%s.json.gz" % (self.file_root, self.dataset_date.strftime("%Y%m%d")))

	def list_files(sp_bucket):
		files = []
		response = s3.list_objects_v2(Bucket=sp_bucket)
		while True:
			files.extend([o['Key'] for o in response['Contents']])
			if not response['IsTruncated']:
				break
		else:
			response = s3.list_objects_v2(Bucket='sp_bucket',
							 ContinuationToken=response['NextContinuationToken'])

		pattern = re.compile(r"xx/enriched/archive/run=" + self.dataset_date.strftime("%Y-%m-%d") + r"-\d{2}-\d{2}-\d{2}/part-\d{5}\.*")		

		for thisfile in files:
			if re.match(pattern, thisfile):
				s3.download_file(Bucket='xx', Key=thisfile)

		return files


	def run(self):
		match_files = self.list_files('sp_bucket')
		s3_filename = "part_%s.%s.json.gz" % (self.file_root, (self.dataset_date + timedelta(days=1)).strftime("%Y-%m-%d"))
		infile_name = self.download_s3_file(s3_file_full_path)
		with gzip.open(self.output().path, "wb") as outfile:
			with gzip.open(infile_name, "rb") as infile:
				for line in infile:
					try:
						indict = json.loads(line)
					except Exception as e:
						logger.warn("s -Could not parse line: %s =%s" % (self.__class__.__name__, e, line))
						continue

						outdict = indict
						
						try:
							outdict['etl_tstamp'] = dateutil.parser.parse(indict['etl_tstamp']).strftime('%Y-%m-%d %H:%M:%S')
						except KeyError:
							pass
						try:
							outdict['collector_tstamp'] = dateutil.parser.parse(indict['collector_tstamp']).strftime('%Y-%m-%d %H:%M:%S')
						except KeyError:
							pass
						try:
							outdict['dvce_created_tstamp'] = dateutil.parser.parse(indict['dvce_created_tstamp']).strftime('%Y-%m-%d %H:%M:%S')
						except KeyError:
							pass
						try:
							outdict['dvce_sent_tstamp'] = dateutil.parser.parse(indict['dvce_sent_tstamp']).strftime('%Y-%m-%d %H:%M:%S')
						except (KeyError, TypeError):
							pass
						try:
							outdict['refr_dvce_tstamp'] = dateutil.parser.parse(indict['refr_dvce_tstamp']).strftime('%Y-%m-%d %H:%M:%S')
						except (KeyError, TypeError):
							pass
						try:
							outdict['derived_tstamp'] = dateutil.parser.parse(indict['derived_tstamp']).strftime('%Y-%m-%d %H:%M:%S')
						except (KeyError, TypeError):
							pass
						try:
							outdict['true_tstamp'] = dateutil.parser.parse(indict['true_tstamp']).strftime('%Y-%m-%d %H:%M:%S')
						except (KeyError, TypeError):
							pass
						
						json.dump(outdict, outfile)
						outfile.write("\n")


class snowplow_enriched_upload_data(luigi.Task):
	dataset_date = luigi.DateParameter(default=date.today() - timedelta(days=1))
	# force_run = luigi.BoolParameter()
	_start = luigi.DateSecondParameter(default=datetime.utcnow())
	file_root = luigi.Parameter()

	credentials = GoogleCredentials.get_application_default()

	def run(self):
		client = gcs.GCSClient(oauth_credentials=self.credentials)
		client.put(self.input().path, self.output().path)

	def output(self):
		return gcs.GCSTarget("gs://snowplow_tracker/%s_%s.json.gz" % (self.file_root, self.dataset_date.strftime("%Y$m%d")))

	def requires(self):
		return process_raw_snowplow_event_data(**self.param_kwargs)



class snowplow_enriched_insert_data(bigquery.BigqueryLoadTask):
	dataset_date = luigi.DateParameter(default=date.today() - timedelta(days=1))
	# force_run = luigi.BoolParameter()
	_start = luigi.DateSecondParameter(default=datetime.utcnow())
	file_root = luigi.Parameter()

	credentials = GoogleCredentials.get_application_default()
	source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
	write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

	def source_uris(self):
		return [x.path for x in luigi.task.flatten(self.input())]

	def output(self):
		return bigquery.BigqueryTarget(
			"xx",
			"snowplow",
			"Events"
			)

	def complete(self):
		return check_partition_modified(
			table="%s.%s" % (self.output().table.dataset_id, self.file_root),
			partition=self.dataset_date.strftime("%Y%m%d"), threshold=60, time_ref=self._start)


	schema = [
		{
			"name": "app_id",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "platform",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Date/Time
		{
			"name": "etl_tstamp",	
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "collector_tstamp",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "dvce_created_tstamp",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Transaction (i.e. this logging event)
		{
			"name": "event",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "event_id",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "txn_id",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Versioning
		{
		
    			"name": "name_tracker",
    			"type": "STRING",
    			"mode": "NULLABLE"
		},
		{
			"name": "v_tracker",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "v_collector",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "v_etl",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#User and Visit
		{
			"name": "user_id",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "user_ipaddress",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "user_fingerprint",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "domain_userid",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "domain_sessionidx",
			"type": "INTEGER",
			"mode": "NULLABLE"
		},
		{
			"name": "network_userid",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Location
		{
			"name": "geo_country",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "geo_region",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "geo_city",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "geo_zipcode",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "geo_latitude",
			"type": "FLOAT",
			"mode": "NULLABLE"
		},
		{
			"name": "geo_longitude",
			"type": "FLOAT",
			"mode": "NULLABLE"
		},
		{
			"name": "geo_region_name",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Other IP Lookups
		{
			"name": "ip_isp",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "ip_organization",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "ip_domain",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "ip_netspeed",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Page
		{
			"name": "page_url",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "page_title",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "page_referrer",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Page URL Components
		{
			"name": "page_urlscheme",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "page_urlhost",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "page_urlport",
			"type": "INTEGER",
			"mode": "NULLABLE"
		},
		{
			"name": "page_urlpath",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "page_urlquery",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "page_urlfragment",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Referrer URL Components
		{
			"name": "refr_urlscheme",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "refr_urlhost",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "refr_urlport",
			"type": "INTEGER",
			"mode": "NULLABLE"
		},
		{
			"name": "refr_urlpath",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "refr_urlquery",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "refr_urlfragment",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Referrer details
		{
			"name": "refr_medium",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "refr_source",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "refr_term",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Marketing
		{
			"name": "mkt_medium",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "mkt_source",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "mkt_term",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "mkt_content",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "mkt_campaign",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Custom Contexts
		{
			"name": "contexts",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Structured Event
		{
			"name": "se_category",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "se_action",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "se_label",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "se_property",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "se_value",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Unstructured Event
		{
			"name": "unstruct_event",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Ecommerce Transaction	
		{
			"name": "tr_orderid",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "tr_affiliation",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "tr_total",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "tr_tax",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "tr_shipping",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "tr_city",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "tr_state",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "tr_country",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Ecommerce transaction item
		{
			"name": "ti_orderid",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "ti_sku",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "ti_name",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "ti_category",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "ti_price",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "ti_quantity",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#PagePing
		{
			"name": "pp_xoffset_min",
			"type": "INTEGER",
			"mode": "NULLABLE"
		},
		{
			"name": "pp_xoffset_max",
			"type": "INTEGER",
			"mode": "NULLABLE"
		},
		{
			"name": "pp_yoffset_min",
			"type": "INTEGER",
			"mode": "NULLABLE"
		},
		{
			"name": "pp_yoffset_max",
			"type": "INTEGER",
			"mode": "NULLABLE"
		},
		#Useragent
		{
			"name": "useragent",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Browser
		{
			"name": "br_name",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "br_family",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "br_version",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "br_type",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "br_renderengine",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "br_lang",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Individual feature fields for non-Hive targets
		{
			"name": "br_features_pdf",
			"type": "BOOLEAN",
			"mode": "NULLABLE"
		},
		{
			"name": "br_features_flash",
			"type": "BOOLEAN",
			"mode": "NULLABLE"
		},
		{
			"name": "br_features_java",
			"type": "BOOLEAN",
			"mode": "NULLABLE"
		},
		{
			"name": "br_features_director",
			"type": "BOOLEAN",
			"mode": "NULLABLE"
		},
		{
			"name": "br_features_quicktime",
			"type": "BOOLEAN",
			"mode": "NULLABLE"
		},
		{
			"name": "br_features_realplayer",
			"type": "BOOLEAN",
			"mode": "NULLABLE"
		},
		{
			"name": "br_features_windowsmedia",
			"type": "BOOLEAN",
			"mode": "NULLABLE"
		},
		{
			"name": "br_features_gears",
			"type": "BOOLEAN",
			"mode": "NULLABLE"
		},
		{
			"name": "br_features_silverlight",
			"type": "BOOLEAN",
			"mode": "NULLABLE"
		},
		{
			"name": "br_cookies",
			"type": "BOOLEAN",
			"mode": "NULLABLE"
		},
		{
			"name": "br_colordepth",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "br_viewwidth",
			"type": "INTEGER",
			"mode": "NULLABLE"
		},
		{
			"name": "br_viewheight",
			"type": "INTEGER",
			"mode": "NULLABLE"
		},
		#OS
		{
			"name": "os_name",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "os_family",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "os_manufacturer",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "os_timezone",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Device/Hardware (from user-agent)
		{
			"name": "dvce_type",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "dvce_ismobile",
			"type": "BOOLEAN",
			"mode": "NULLABLE"
		},
		#Device (from querystring)
		{
			"name": "dvce_screenwidth",
			"type": "INTEGER",
			"mode": "NULLABLE"
		},
		{
			"name": "dvce_screeenheight",
			"type": "INTEGER",
			"mode": "NULLABLE"
		},
		#Document
		{
			"name": "doc_charset",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "doc_width",
			"type": "INTEGER",
			"mode": "NULLABLE"
		},
		{
			"name": "doc_height",
			"type": "INTEGER",
			"mode": "NULLABLE"
		},
		#Currency
		{
			"name": "tr_currency",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "tr_total_base",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "tr_tax_base",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "tr_shipping_base",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "ti_currency",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "ti_price_base",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "base_currency",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		# Geolocation
		{
			"name": "geo_timezone",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Click ID
		{
			"name": "mkt_clickid",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "mkt_network",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#ETL tags
		{
			"name": "etl_tags",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Time event was sent
		{
			"name": "dvce_sent_tstamp",
			"type": "STRING",
			"mode": "NULLABLE"	
		},
		#Referrer
		{
			"name": "refr_domain_userid",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "refr_dvce_tstamp",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Dervied context
		{
			"name": "derived_contexts",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Session ID
		{
			"name": "domain_sessionid",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Derived timestamp
		{
			"name": "derived_tstamp",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Derived event vendor/name/format/version
		{
			"name": "event_vendor",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "event_name",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "event_format",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		{
			"name": "event_version",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Event Fingerprint
		{
			"name": "event_fingerprint",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#True timestamp
		{
			"name": "true_tstamp",
			"type": "STRING",
			"mode": "NULLABLE"
		},
		#Fields modified in PII enrichment (JSON String)
		{
			"name": "pii",
			"type": "STRING",
			"mode": "NULLABLE"
		}
	]

	def requires(self):
		return snowplow_enriched_upload_data(**self.param_kwargs)	


	

if __name__ == '__main__':
	import timeit
	start_time = timeit.default_timer()
	luigi.run()
	elapsed = timeit.default_timer() - start_time
	print("Snowplow data successfully uploaded - Time for excecution: %s seconds." % int(elapsed))
