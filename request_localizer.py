#!/usr/bin/env python 
from __future__ import division
from builtins import str
from builtins import range
from builtins import object
import os, sys, time, json, requests, logging, re
import hashlib
from datetime import datetime
from dateutil import parser
from hysds_commons.job_utils import resolve_hysds_job
from hysds.celery import app
import util
import uuid  # only need this import to simulate returned mozart job id
from hysds.celery import app
from hysds_commons.job_utils import submit_mozart_job
import traceback
from fetchOrbitES import fetch
try:
    import acquisition_localizer_multi
except:
    import multi_acquisition_localizer.acquisition_localizer_multi



# set logger
log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True

logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())

#"match_pattern": "/(?P<id>S1-GUNW-ifg-cfg-RM-M\\w+S\\w+-TN\\w+-\\d{8}T\\d{6}-\\d{8}T\\d{6}-poeorb-\\w{4})$"
IFG_CFG_ID_TMPL = "S1-GUNW-ifg-cfg-R{}-M{:d}S{:d}-TN{:03d}-{:%Y%m%dT%H%M%S}-{:%Y%m%dT%H%M%S}-{}-{}"
REQUEST_IFG_CFG_ID_TMPL = "runconfig-topsapp-{}-TN{:03d}-{:%Y%m%dT%H%M%S}-{:%Y%m%dT%H%M%S}-{}"

SLC_RE = re.compile(r'(?P<mission>S1\w)_IW_SLC__.*?' +
                    r'_(?P<start_year>\d{4})(?P<start_month>\d{2})(?P<start_day>\d{2})' +
                    r'T(?P<start_hour>\d{2})(?P<start_min>\d{2})(?P<start_sec>\d{2})' +
                    r'_(?P<end_year>\d{4})(?P<end_month>\d{2})(?P<end_day>\d{2})' +
                    r'T(?P<end_hour>\d{2})(?P<end_min>\d{2})(?P<end_sec>\d{2})_.*$')

BASE_PATH = os.path.dirname(__file__)
MOZART_URL = app.conf['MOZART_URL']
MOZART_ES_ENDPOINT = "MOZART"
GRQ_ES_ENDPOINT = "GRQ"
sleep_seconds = 120
slc_check_max_sec = 300
sling_completion_max_sec = 11000


class ACQ(object):
    def __init__(self, acq_id, acq_type):
        self.acq_id=acq_id
        self.acq_type = acq_type

def get_acq_object(acq_id, acq_type):
    return {
        "acq_id": acq_id,
        "acq_type":  acq_type
    }
def get_job_object(job_type, job_id, completed):
    return {
        "job_id": job_id,
        "job_type":  job_type,
        "completed" : completed
    }

def get_area(coords):
    '''get area of enclosed coordinates- determines clockwise or counterclockwise order'''
    n = len(coords) # of corners
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        area += coords[i][1] * coords[j][0]
        area -= coords[j][1] * coords[i][0]
    #area = abs(area) / 2.0
    return area / 2

def get_orbit_from_orbit_file(orbit_file):
    logger.info("get_orbit_from_orbit_file : {}".format(orbit_file))
    es_url = app.conf["GRQ_ES_URL"]
    es_index = "grq"
    query = {
       "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "metadata.archive_filename.raw": orbit_file
                         }
                    }
                ]
            }
        }
    }

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()

    if len(result["hits"]["hits"]) == 0:
        raise ValueError("Couldn't find record with orbit file: %s, at ES: %s"%(orbit_file, es_url))
        return

    #LOGGER.debug("Got: {0}".format(json.dumps(result)))
    h = result["hits"]["hits"][0]
    fields = h['_source']
    prod_url = fields['urls'][0]
    if len(fields['urls']) > 1:
        for u in fields['urls']:
            if u.startswith('http://'):
                prod_url = u
                break

    orbit_url = os.path.join(prod_url, orbit_file)
    logger.info("get_orbit_from_orbit_file : orbit_url : {}".format(orbit_url))
    return orbit_url

def query_es(endpoint, doc_id):
    """
    This function queries ES
    :param endpoint: the value specifies which ES endpoint to send query
     can be MOZART or GRQ
    :param doc_id: id of product or job
    :return: result from elasticsearch
    """
    es_url, es_index = None, None
    if endpoint == GRQ_ES_ENDPOINT:
        es_url = app.conf["GRQ_ES_URL"]
        es_index = "grq"
    if endpoint == MOZART_ES_ENDPOINT:
        es_url = app.conf['JOBS_ES_URL']
        es_index = "job_status-current"

    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"_id": doc_id}} # add job status:
                ]
            }
        }
    }

    #ES = elasticsearch.Elasticsearch(es_url)
    #result = ES.search(index=es_index, body=query)

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()

    if len(result["hits"]["hits"]) == 0:
        raise ValueError("Couldn't find record with ID: %s, at ES: %s"%(doc_id, es_url))
        return

    #LOGGER.debug("Got: {0}".format(json.dumps(result)))
    return result["hits"]["hits"][0]

def create_dataset_json(id, version, met_file, ds_file):
    """Write dataset json."""

    logger.info("create_dataset_json")

    # get metadata
    with open(met_file) as f:
        md = json.load(f)

    ds = {
        'creation_timestamp': "%sZ" % datetime.utcnow().isoformat(),
        'version': version,
        'label': id
    }

    coordinateste_dataset_json = None

    try:

        coordinates = md['union_geojson']['coordinates']
    
        cord_area = get_area(coordinates[0])
        if not cord_area>0:
            logger.info("creating dataset json. coordinates are not clockwise, reversing it")
            coordinates = [coordinates[0][::-1]]
            logger.info(coordinates)
            cord_area = get_area(coordinates[0])
            if not cord_area>0:
                logger.info("creating dataset json. coordinates are STILL NOT  clockwise")
        else:
            logger.info("creating dataset json. coordinates are already clockwise")

        ds['location'] =  {'type': 'Polygon', 'coordinates': coordinates}
        logger.info("create_dataset_json location : %s" %ds['location'])

    except Exception as err:
        logger.info("create_dataset_json: Exception : ")
        logger.warn(str(err))
        logger.warn("Traceback: {}".format(traceback.format_exc()))


    ds['starttime'] = md['starttime']
    ds['endtime'] = md['endtime']

    # write out dataset json
    with open(ds_file, 'w') as f:
        json.dump(ds, f, indent=2)

def get_job_status(job_id):
    """
    This function gets the staged products and context of previous PGE job
    :param job_id: this is the id of the job on mozart
    :return: tuple(products_staged, prev_context, message)
    the message refects the
    """
    endpoint = MOZART_ES_ENDPOINT
    return_job_id = None
    return_job_status = None

    #check if Jobs ES has updated job status
    if check_ES_status(job_id):
        response = query_es(endpoint, job_id)

    result = response["hits"]["hits"][0]
    message = None  #using this to store information regarding deduped jobs, used later to as error message unless it's value is "success"

    #print ("Job INFO retrieved from ES: %s"%json.dumps(result))
    #print ("Type of status from ES: %s"%type(result["_source"]["status"]))
    status = str(result["_source"]["status"])
    if status == "job-deduped":
        #query ES for the original job's status
        orig_job_id = result["_source"]["dedup_job"]
        return_job_id = orig_job_id
        orig_job_info = query_es(endpoint, orig_job_id)
        """check if original job failed -> this would happen when at the moment of deduplication, the original job
         was in 'running state', but soon afterwards failed. So, by the time the status is checked in this function,
         it may be shown as failed."""
        #print ("Original JOB info: \n%s"%json.dumps(orig_job_info))
        orig_job_info = orig_job_info["hits"]["hits"][0]
        orig_job_status = str(orig_job_info["_source"]["status"])
        logger.info("Check Job Status : Job %s was Deduped. The new/origianl job id is %s whose status is : %s" %(job_id, return_job_id, return_job_status))
        return_job_status = orig_job_status

        if  orig_job_status == "job-failed":
            message = "Job was deduped against a failed job with id: %s, please retry job."%orig_job_id
            logger.info(message) 
        elif orig_job_status == "job-started" or orig_job_status == "job-queued":
            logger.info ("Job was deduped against a queued/started job with id: %s. Please look at already running job with same params."%orig_job_id)
            message = "Job was deduped against a queued/started job with id: %s. Please look at already running job with same params."%orig_job_id
        elif orig_job_status == "job-completed":
            # return products staged and context of original job
            message = "success"
    else:
        return_job_id = job_id
        return_job_status = result["_source"]["status"]

    return return_job_status, return_job_id

def check_ES_status(doc_id):
    """
    There is a latency in the update of ES job status after
    celery signals job completion.
    To handle that case, we much poll ES (after sciflo returns status after blocking)
    until the job status is correctly reflected.
    :param doc_id: ID of the Job ES doc
    :return: True  if the ES has updated job status within 5 minutes
            otherwise raise a run time error
    """
    es_url = app.conf['JOBS_ES_URL']
    es_index = "job_status-current"
    query = {
        "_source": [
                   "status"
               ],
        "query": {
            "bool": {
                "must": [
                    {"term": {"_id": doc_id}}
                ]
            }
        }
    }

    #ES = elasticsearch.Elasticsearch(es_url)
    #result = ES.search(index=es_index, body=query)
    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()


    sleep_seconds = 2
    timeout_seconds = 300
    # poll ES until job status changes from "job-started" or for the job doc to show up. The poll will timeout soon after 5 mins.

    while len(result["hits"]["hits"]) == 0: #or str(result["hits"]["hits"][0]["_source"]["status"]) == "job-started":
        if sleep_seconds >= timeout_seconds:
            if len(result["hits"]["hits"]) == 0:
                raise RuntimeError("ES taking too long to index job with id %s."%doc_id)
            else:
                raise RuntimeError("ES taking too long to update status of job with id %s."%doc_id)
        time.sleep(sleep_seconds)
        #result = ES.search(index=es_index, body=query)

        r = requests.post(search_url, data=json.dumps(query))

        if r.status_code != 200:
            print("Failed to query %s:\n%s" % (es_url, r.text))
            print("query: %s" % json.dumps(query, indent=2))
            print("returned: %s" % r.text)
            r.raise_for_status()

        result = r.json()
        sleep_seconds = sleep_seconds * 2

    logging.info("Job status updated on ES to %s"%str(result["hits"]["hits"][0]["_source"]["status"]))
    return True

def check_slc_status(slc_id, index_suffix):

    result = util.get_dataset(slc_id, index_suffix)
    total = result['hits']['total']

    if total > 0:
        return True

    return False

def check_slc_status(slc_id):

    result = util.get_dataset(slc_id)
    total = result['hits']['total']

    if total > 0:
        return True

    return False

def get_acq_data_from_list(acq_list):
    logger.info("get_acq_data_from_list")
    acq_info = {}
    slcs = []
    
    # Find out status of all Master ACQs, create a ACQ object with that and update acq_info dictionary 
    for acq in acq_list: 
        acq_data = util.get_partial_grq_data(acq)['fields']['partial'][0] 
        slcs.append(acq_data['metadata']['identifier'])
        '''
        status = check_slc_status(acq_data['metadata']['identifier']) 
        if status: 
            # status=1 
            logger.info("%s exists" %acq_data['metadata']['identifier']) 
            acq_info[acq]=get_acq_object(acq, acq_data, 1) 
        else: 
            #status = 0 
            logger.info("%s does NOT exist"%acq_data['metadata']['identifier']) 
            acq_info[acq]=get_acq_object(acq, acq_data, 0)
        '''
    #return acq_info
    return slcs


def resolve_source(ctx_file):
    """Resolve best URL from acquisition."""


    # get settings
    # read in context
    with open(ctx_file) as f:
        ctx = json.load(f)
    

    # build args
    project = util.get_value(ctx, "project", "aria")
    if type(project) is list:
        project = project[0]

    job_type, job_version = ctx['job_specification']['id'].split(':')
    input_metadata = ctx["input_metadata"]
  
    esa_download_queue = util.get_value(ctx, "esa_download_queue", "slc-sling-extract-local-scihub")
    asf_ngap_download_queue = util.get_value(ctx, "asf_ngap_download_queue", "slc-sling-extract-local-asf")
    spyddder_sling_extract_version = util.get_value(ctx, "spyddder_sling_extract_version", "ARIA-446_singularity")
    multi_acquisition_localizer_version = util.get_value(ctx, "multi_acquisition_localizer_version", "ARIA-446")
    destination_type = ctx.get("destination_type", "local")
    request_id = ctx.get('request_id', None)

    return process_acqlist_localization(input_metadata, esa_download_queue, asf_ngap_download_queue, spyddder_sling_extract_version, multi_acquisition_localizer_version, job_type, job_version, project, destination_type)
    

def process_acqlist_localization(input_metadata, esa_download_queue, asf_ngap_download_queue, spyddder_sling_extract_version, multi_acquisition_localizer_version, job_type, job_version, project, destination_type="local", request_id=None):
    logger.info("process_acqlist_localization")
    dem_type= input_metadata["dem_type"]
    track = input_metadata["track_number"]

    master_acqs = input_metadata["master_acquisitions"]
    slave_acqs = input_metadata["slave_acquisitions"]

    master_scene = input_metadata["master_scenes"]   
    slave_scene = input_metadata["slave_scenes"]
    logger.info("master_scene : %s" %master_scene)
    logger.info("slave_scene : %s" %slave_scene)
    logger.info("master_acqs : %s" %master_acqs)
    logger.info("slave_acqs : %s" %slave_acqs)

    starttime = input_metadata["starttime"]
    endtime = input_metadata["endtime"]
    bbox = None
    if "bbox" in input_metadata:
        bbox = input_metadata["bbox"]

    #esa_download_queue = "slc-sling-extract-scihub"
    #asf_ngap_download_queue = "slc-sling-extract-asf"

    union_geojson = input_metadata["union_geojson"]
    direction = input_metadata["direction"] 
    platform = input_metadata["platform"]

    job_priority = input_metadata["job_priority"]

    orbitNumber = []
    if "orbitNumber" in input_metadata:
        orbitNumber = input_metadata["orbitNumber"]


    acq_info = {}
   
    for acq in master_acqs:
        acq_type = "master"
        acq_info[acq]=get_acq_object(acq, acq_type)

    # Find out status of all Slave ACQs, create a ACQ object with that and update acq_info dictionary
    for acq in slave_acqs:
        acq_type = "slave"
        acq_info[acq]=get_acq_object(acq, acq_type)

    return sling(acq_info, spyddder_sling_extract_version, multi_acquisition_localizer_version, project, job_priority, job_type, job_version, dem_type, track, starttime, endtime, master_acqs, slave_acqs, orbitNumber, direction, platform, union_geojson, bbox, esa_download_queue, asf_ngap_download_queue, destination_type, request_id)

def sling(acq_info, spyddder_sling_extract_version, multi_acquisition_localizer_version, project, job_priority, job_type, job_version, dem_type, track, starttime, endtime, master_acqs, slave_acqs, orbitNumber, direction, platform, union_geojson, bbox, esa_download_queue, asf_ngap_download_queue, destination_type="local", request_id=None):
    '''
	This function submits acquisition localizer jobs for mastrer and slaves.
    '''
    logger.info("SLING: %s : %s" %(type(spyddder_sling_extract_version), spyddder_sling_extract_version))
    job_info = {}
    
    acq_list = list(acq_info.keys())

    logger.info("spyddder_sling_extract_version : %s" %spyddder_sling_extract_version)
    logger.info("multi_acquisition_localizer_version : %s" %multi_acquisition_localizer_version)
    logger.info("master_acqs: %s" %master_acqs)
    logger.info("slave_acqs : %s" %slave_acqs)
    logger.info("job_priority : %s" %job_priority)

    logger.info("\nSubmitting acquisition localizer job for Masters : %s" %master_acqs)
    all_done, data  = submit_sling_job(spyddder_sling_extract_version, multi_acquisition_localizer_version, master_acqs, job_priority, esa_download_queue, asf_ngap_download_queue, destination_type, request_id)
    if not all_done:
        err_str = "Failed to download following SLCs :"
        for failed_acq_id in data:
            err_str+="\n%s" %failed_acq_id
            raise RuntimeError(err_str)
    else:
        logger.info("successfully completed localizing master slcs")
            

    logger.info("\nSubmitting acquisition localizer job for Slaves : %s" %slave_acqs)
    all_done, data = submit_sling_job(spyddder_sling_extract_version, multi_acquisition_localizer_version, slave_acqs, job_priority, esa_download_queue, asf_ngap_download_queue)
    if not all_done:
        err_str = "Failed to download following SLCs :"
        for failed_acq_id in data:
            err_str+="\n%s" %failed_acq_id
            raise RuntimeError(err_str)
    else:
        logger.info("successfully completed localizing slave slcs")


    logger.info("Sling Jobs Submission Done. Returning")
    return

def get_id_hash(acq_info, job_priority, dem_type):
    id_hash = ""
    master_ids_str=""
    slave_ids_str=""
    master_slcs = []
    slave_slcs = []

    for acq in sorted(acq_info.keys()):
        acq_type = acq_info[acq]['acq_type']
        master_slcs.append(acq_info[acq]['acq_type'])
        if acq_type == "master":
            if master_ids_str=="":
                master_ids_str=acq
            else:
                master_ids_str += " "+acq

        elif acq_type == "slave":
                if slave_ids_str=="":
                    slave_ids_str=acq
                else:
                    slave_ids_str += " "+acq


    logger.info("master_ids_str : %s" %master_ids_str)
    logger.info("slave_ids_str : %s" %slave_ids_str)

       
    id_hash = hashlib.md5(json.dumps([
	master_ids_str,
	slave_ids_str,
        dem_type
    ]).encode("utf8")).hexdigest()

    return id_hash

def get_urls(info):
    """Return list of SLC URLs with preference for S3 URLs."""

    urls = []
    for id in info:
        h = info[id]
        fields = h['_source']
        prod_url = fields['urls'][0]
        if len(fields['urls']) > 1:
            for u in fields['urls']:
                if u.startswith('s3://'):
                    prod_url = u
                    break
        urls.append("%s/%s" % (prod_url, fields['metadata']['archive_filename']))
    return urls

        

def check_all_job_completed(job_info):
    all_done = True
    for job_id in list(job_info.keys()):
        if not job_info[job_id]['completed']:  
            logger.info("\ncheck_all_job_completed : %s NOT completed!!" %job_id)
            all_done = False
            break
    return all_done


def get_dem_type(info):
    """Get dem type."""

    dem_type = "SRTM+v3"

    dems = {}
    for id in info:
        dem_type = "SRTM+v3"
        h = info[id]
        fields = h["_source"]
        try:
            if 'city' in fields:
                for city in fields['city']:
                    if city['country_name'] is not None:
                        if city['country_name'].lower() == "united states":
                            dem_type="Ned1"
                            print("Found city in US : %s. So dem type is ned" %fields['city'][0]['country_name'].lower())
                            break
                    else:
                        print("fields['city'][0]['country_name'] is None")
        except:
            dem_type = "SRTM+v3"
        if dem_type.upper().startswith("NED"):
            break

    return dem_type



def get_orbit_from_ids(ids, scene_type="slc"):
    """Get orbit for a set of SLC ids. They need to belong to the same day."""

    day_dt, all_dts, mission = util.get_date_from_ids(ids, scene_type)
    return fetch("%s.0" % all_dts[0].isoformat(), "%s.0" % all_dts[-1].isoformat(),
                 mission=mission, dry_run=True)

def get_orbit_from_metadata(mds):
    """Get orbit for a set of SLC ids. They need to belong to the same day."""

    day_dt, all_dts, mission = util.get_date_from_metadata(mds)
    logger.info("get_orbit_from_metadata : day_dt %s, all_dts %s, mission %s" %(day_dt, all_dts, mission))
    return fetch("%s.0" % all_dts[0].isoformat(), "%s.0" % all_dts[-1].isoformat(),
                 mission=mission, dry_run=True)

def update_metadata_for_runconfig(md, tag_list):
    '''
    if it is request job, get request_product and update metadata
    '''
    
    md["tags"] = tag_list
    request_id = util.get_request_id(tag_list)
    if not request_id:
        err_msg = "No request id found in runconfig-acqlist with tags  : {}".format(tag_list)
        logger.info(err_msg)
        raise Exception(err_msg)

    #Search GRQ for request metadata
    data = query_es(GRQ_ES_ENDPOINT, request_id)
    md["request_metadata"] = data["metadata"]

    return md


def publish_localized_info( acq_info, project, job_priority, dem_type, track, aoi_id, starttime, endtime, master_scene, slave_scene, orbitNumber, direction, platform, union_geojson, bbox, tag_list = [], wuid=None, job_num=None):
    for i in range(len(project)):
        publish_data( acq_info[i], project[i], job_priority[i], dem_type[i], track[i], aoi_id[i],  starttime[i], endtime[i], master_scene[i], slave_scene[i], orbitNumber[i], direction[i], platform[i], union_geojson[i], bbox[i], tag_list[i])

def publish_ifgcfg_data( acq_info, project, job_priority, dem_type, track, aoi_id, starttime, endtime, master_scene, slave_scene, master_acqs, slave_acqs, orbitNumber, direction, platform, union_geojson, bbox, ifg_hash, in_master_orbit_file, in_slave_orbit_file, tag_list = [], wuid=None, job_num=None):
    """Map function for create interferogram job json creation."""

    id, md = get_ifgcfg_metadata( acq_info, project, job_priority, dem_type, track, aoi_id, starttime, endtime, master_scene, slave_scene, master_acqs, slave_acqs, orbitNumber, direction, platform, union_geojson, bbox, ifg_hash, in_master_orbit_file, in_slave_orbit_file)
    
    return publish_dataset(id, md)

def publish_dataset(id, md, version="v2.0"):
    prod_dir =  id
    if not os.path.isdir(prod_dir):
        os.makedirs(prod_dir, 0o755)

    met_file = os.path.join(prod_dir, "{}.met.json".format(id))
    ds_file = os.path.join(prod_dir, "{}.dataset.json".format(id))


    with open(met_file, 'w') as f: json.dump(md, f, indent=2)
    

    print("creating dataset file : %s" %ds_file)
    create_dataset_json(id, version, met_file, ds_file)

    return prod_dir


def publish_topsapp_runconfig_data( acq_info, project, job_priority, dem_type, track, aoi_id, starttime, endtime, master_scene, slave_scene, master_acqs, slave_acqs, orbitNumber, direction, platform, union_geojson, bbox, ifg_hash, in_master_orbit_file, in_slave_orbit_file, tag_list = [], wuid=None, job_num=None):
    """Map function for create interferogram job json creation."""

    id, md = get_ifgcfg_metadata( acq_info, project, job_priority, dem_type, track, aoi_id, starttime, endtime, master_scene, slave_scene, master_acqs, slave_acqs, orbitNumber, direction, platform, union_geojson, bbox, ifg_hash, in_master_orbit_file, in_slave_orbit_file, True, tag_list)

    request_id = util.get_request_id(tag_list)
    
    if not request_id:
        err_msg = "No request id found in runconfig-acqlist with tags  : {}".format(tag_list)
        logger.info(err_msg)
        raise Exception(err_msg)
    
    request_submit_id = request_id.replace("request", "request-submit", 1)
   
    #Search GRQ for request metadata
    data = query_es(GRQ_ES_ENDPOINT, request_submit_id)
    request_md =  data["_source"]["metadata"]
    logger.info("data : {}".format(json.dumps(data, indent = 2)))
    md["request_metadata"] = request_md
    md["tags"] = request_md["tags"]
    md["geocoded_unfiltered_coherence"] = request_md["geocoded_unfiltered_coherence"]
    md["geocoded_unfiltered_wrapped_phase"] = request_md["geocoded_unfiltered_wrapped_phase"]
 
    
    return publish_dataset(id, md)


def get_ifgcfg_metadata( acq_info, project, job_priority, dem_type, track, aoi_id, starttime, endtime, master_scene, slave_scene, master_acqs, slave_acqs, orbitNumber, direction, platform, union_geojson, bbox, ifg_hash, in_master_orbit_file, in_slave_orbit_file, is_request = False, tag_list = []):
    logger.info("\n\n\n PUBLISH IFG JOB!!!")
    logger.info("project : %s " %project)
    logger.info("dem type : %s " %dem_type)
    logger.info("track : %s" %track)
    logger.info("aoi_id : %s" %aoi_id)
    logger.info("starttime, endtime, : %s : %s " %(starttime, endtime))
    logger.info("master_scene, slave_scene : %s, %s" %(master_scene, slave_scene))
    logger.info("union_geojson : %s, bbox : %s " %( union_geojson, bbox))
    logger.info("publish_data : orbitNumber : %s" %orbitNumber)
    #version = get_version()
    version = "v2.0.0"

    if type(project) is list:
        project = project[0]
    logger.info("project : %s" %project)
    
    # set job type and disk space reqs
    disk_usage = "100GB"

    # set job queue based on project
    job_queue = "request_s1ifg-slc_localizer"

    # get metadata
    master_md = { i:query_es(GRQ_ES_ENDPOINT, i) for i in master_scene}
    #logger.info("master_md: {}".format(json.dumps(master_md, indent=2)))
    slave_md = { i:query_es(GRQ_ES_ENDPOINT, i) for i in slave_scene }
    #logger.info("slave_md: {}".format(json.dumps(slave_md, indent=2)))

    # get urls (prefer s3)
    logger.info("master_md : %s" %master_md)
    logger.info("slave_md : %s" %slave_md)
    master_zip_url = util.get_urls(master_md) 
    logger.info("master_zip_url: {}".format(master_zip_url))
    slave_zip_url = util.get_urls(slave_md) 
    logger.info("slave_ids: {}".format(slave_zip_url))

    # get orbits
    master_orbit_file = None
    master_orbit_url = get_orbit_from_metadata(master_md)
    logger.info("master_orbit_url: {}".format(master_orbit_url))
    if master_orbit_url:
        master_orbit_file = os.path.basename(master_orbit_url)
    else:
        raise RuntimeError("Reference Orbit File NOT Found")
        '''
        master_orbit_file = in_master_orbit_file
        master_orbit_url = get_orbit_from_orbit_file(in_master_orbit_file)
        '''

    slave_orbit_file = None
    slave_orbit_url = get_orbit_from_metadata(slave_md)
    if slave_orbit_url:
        slave_orbit_file = os.path.basename(slave_orbit_url)
    else:
        raise RuntimeError("Secondery Orbit File NOT Found")
    '''
    else:
        slave_orbit_file = in_slave_orbit_file
        slave_orbit_url = get_orbit_from_orbit_file(in_slave_orbit_file)
    '''
    logger.info("slave_orbit_url: {}".format(slave_orbit_url))

    try:
        dem_type = get_dem_type(master_md)
    except:
        pass
 
    slc_master_dt, slc_slave_dt = util.get_scene_dates_from_metadata(master_md, slave_md) 
    

    # set localize urls
    localize_urls = [
        { 'url': master_orbit_url },
        { 'url': slave_orbit_url },
    ]
    for m in master_zip_url: localize_urls.append({'url': m})
    for s in slave_zip_url: localize_urls.append({'url': s})



    logger.info(" master_scene : %s slave_slcs : %s" %(master_scene, slave_scene))
    orbit_type = 'poeorb'
    logger.info("Publish IFG job: direction : %s, platform : %s" %(direction, platform))

    id = IFG_CFG_ID_TMPL.format('M', len(master_scene), len(slave_scene), track, parser.parse(slc_master_dt.strftime('%Y%m%dT%H%M%S')), parser.parse(slc_slave_dt.strftime('%Y%m%dT%H%M%S')), orbit_type, ifg_hash[0:4])

    if is_request:
        request_id = util.get_request_id(tag_list)
        id = REQUEST_IFG_CFG_ID_TMPL.format(request_id, track, parser.parse(slc_master_dt.strftime('%Y%m%dT%H%M%S')), parser.parse(slc_slave_dt.strftime('%Y%m%dT%H%M%S')), ifg_hash[0:4])

    logger.info("master_orbit_file : %s" %master_orbit_file)
    logger.info("slave_orbit_file : %s" %slave_orbit_file)
 
    #with open(met_file) as f: md = json.load(f)
    md = {}
    md['id'] = id
    md['project'] =  project,
    md['priority'] = job_priority
    md['azimuth_looks'] = 7
    md['range_looks'] = 19
    md['filter_strength'] =  0.5
    md['precise_orbit_only'] = 'true'
    md['auto_bbox'] = 'true'
    md['_disk_usage'] = disk_usage
    md['soft_time_limit'] =  86400
    md['time_limit'] = 86700
    md['dem_type'] = dem_type
    md['track_number'] = track
    md['starttime'] = starttime
    md['endtime'] = endtime
    md['union_geojson'] = union_geojson
    md['master_scenes'] = master_scene
    md['slave_scenes'] = slave_scene
    md["master_acquisitions"] = master_acqs
    md["slave_acquisitions"] = slave_acqs
    md['orbitNumber'] = orbitNumber
    md['direction'] = direction
    md['platform'] = platform
    md['master_orbit_url'] = master_orbit_url
    md['slave_orbit_url'] = slave_orbit_url
    md['master_zip_url'] = master_zip_url
    md['slave_zip_url'] = slave_zip_url
    md['localize_urls'] = localize_urls
    md['slc_master_dt'] = slc_master_dt.strftime('%Y%m%dT%H%M%S')
    md['slc_slave_dt'] = slc_slave_dt.strftime('%Y%m%dT%H%M%S')
    md["master_zip_file"] = [os.path.basename(i) for i in master_zip_url]
    md["master_orbit_file"] = os.path.basename(master_orbit_url)
    md["slave_zip_file"] = [os.path.basename(i) for i in slave_zip_url]
    md["slave_orbit_file"] = os.path.basename(slave_orbit_url)
    md["full_id_hash"] = ifg_hash
    md["id_hash"] = ifg_hash[0:4]
    if bbox:
        md['bbox'] = bbox

    return id, md

def submit_sling_job(spyddder_sling_extract_version, multi_acquisition_localizer_version, acq_list, priority, esa_download_queue, asf_ngap_download_queue, destination_type="local", request_id=None):
    #esa_download_queue = "slc-sling-extract-scihub"
    #asf_ngap_download_queue = "slc-sling-extract-asf"
    job_type = "job-acquisition_localizer_multi:{}".format(multi_acquisition_localizer_version)
    job_version = multi_acquisition_localizer_version
    logger.info("submit_sling_job : acq_list %s, \nspyddder_sling_extract_version : %s, \nmulti_acquisition_localizer_version: %s, \nesa_download_queue : %s, \nasf_ngap_download_queue: %s, \npriority : %s, \njob_type : %s, \njob_version : %s" %(acq_list, spyddder_sling_extract_version, multi_acquisition_localizer_version, esa_download_queue, asf_ngap_download_queue, priority, job_type, job_version))




    try:
        return acquisition_localizer_multi.sling(acq_list, spyddder_sling_extract_version, multi_acquisition_localizer_version, esa_download_queue, asf_ngap_download_queue, priority, job_type, job_version, destination_type, request_id)
    except Exception as e:
        logger.info("Error processing standard product localizer : %s" %str(e))
        traceback.format_exc()
        raise RuntimeError("Error processing standard product localizer : %s" %str(e))

    return


def check_ES_status(doc_id):
    """
    There is a latency in the update of ES job status after
    celery signals job completion.
    To handle that case, we much poll ES (after sciflo returns status after blocking)
    until the job status is correctly reflected.
    :param doc_id: ID of the Job ES doc
    :return: True  if the ES has updated job status within 5 minutes
            otherwise raise a run time error
    """
    es_url = app.conf['JOBS_ES_URL']
    es_index = "job_status-current"
    query = {
        "_source": [
                   "status"
               ],
        "query": {
            "bool": {
                "must": [
                    {"term": {"_id": doc_id}}
                ]
            }
        }
    }

    #ES = elasticsearch.Elasticsearch(es_url)
    #result = ES.search(index=es_index, body=query)
    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    r = requests.post(search_url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    result = r.json()


    sleep_seconds = 2
    timeout_seconds = 300
    # poll ES until job status changes from "job-started" or for the job doc to show up. The poll will timeout soon after 5 mins.

    while len(result["hits"]["hits"]) == 0: #or str(result["hits"]["hits"][0]["_source"]["status"]) == "job-started":
        if sleep_seconds >= timeout_seconds:
            if len(result["hits"]["hits"]) == 0:
                raise RuntimeError("ES taking too long to index job with id %s."%doc_id)
            else:
                raise RuntimeError("ES taking too long to update status of job with id %s."%doc_id)
        time.sleep(sleep_seconds)
        #result = ES.search(index=es_index, body=query)

        r = requests.post(search_url, data=json.dumps(query))

        if r.status_code != 200:
            print("Failed to query %s:\n%s" % (es_url, r.text))
            print("query: %s" % json.dumps(query, indent=2))
            print("returned: %s" % r.text)
            r.raise_for_status()

        result = r.json()
        sleep_seconds = sleep_seconds * 2

    logging.info("Job status updated on ES to %s"%str(result["hits"]["hits"][0]["_source"]["status"]))
    return True
    

def main():
    context_file = os.path.abspath("_context.json")
    if not os.path.exists(context_file):
        raise RuntimeError
    resolve_source(context_file)

if __name__ == "__main__":
    main()
