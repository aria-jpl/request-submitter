#!/usr/bin/env python3 
from __future__ import division
from builtins import str
import os, sys, time, json, requests, logging
import re, traceback, argparse, copy, bisect
from xml.etree import ElementTree
from UrlUtils import UrlUtils
import util
import gtUtil
from util import ACQ, InvalidOrbitException
import datetime  
from datetime import datetime, timedelta
import groundTrack
from osgeo import ogr
import lightweight_water_mask
import csv
from dateutil import parser

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

SLC_RE = re.compile(r'(?P<mission>S1\w)_IW_SLC__.*?' +
                    r'_(?P<start_year>\d{4})(?P<start_month>\d{2})(?P<start_day>\d{2})' +
                    r'T(?P<start_hour>\d{2})(?P<start_min>\d{2})(?P<start_sec>\d{2})' +
                    r'_(?P<end_year>\d{4})(?P<end_month>\d{2})(?P<end_day>\d{2})' +
                    r'T(?P<end_hour>\d{2})(?P<end_min>\d{2})(?P<end_sec>\d{2})_.*$')

BASE_PATH = os.path.dirname(__file__)
MISSION = 'S1A'
DEFAULT_AOI = "DefaultAOI"

def query_es(query, es_index=None):
    logger.info("query: %s" %query)
    """Query ES."""
    uu = UrlUtils()
    es_url = uu.rest_url
    rest_url = es_url[:-1] if es_url.endswith('/') else es_url
    url = "{}/_search?search_type=scan&scroll=60&size=100".format(rest_url)
    if es_index:
        url = "{}/{}/_search?search_type=scan&scroll=60&size=100".format(rest_url, es_index)
    logger.info("url: {}".format(url))
    r = requests.post(url, data=json.dumps(query))
    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    scan_result = r.json()
    #logger.info("scan_result: {}".format(json.dumps(scan_result, indent=2)))
    count = scan_result['hits']['total']
    if count == 0:
        return []


    if '_scroll_id' not in scan_result:
        logger.info("_scroll_id not found in scan_result. Returning empty array for the query :\n%s" %query)
        return []

    scroll_id = scan_result['_scroll_id']
    hits = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=60m' % rest_url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        hits.extend(res['hits']['hits'])
    return hits




def query_aois(starttime, endtime):
    """Query ES for active AOIs that intersect starttime and endtime."""

    es_index = "grq_*_area_of_interest"
    query = {
        "query": {
            "bool": {
                "should": [
                    {
                        "bool": {
                            "must": [
                                {
                                    "range": {
                                        "starttime": {
                                            "lte": endtime
                                        }
                                    }
                                },
                                {
                                    "range": {
                                        "endtime": {
                                            "gte": starttime
                                        }
                                    }
                                },
				{
                                    "match": {
					    "dataset_type": "area_of_interest"
                                        }
                                }
                            ]
                        }
                    },
        {
        "filtered": {
            "query": {

              "bool": {
                "must": [
                  {
                    "match": {
                      "dataset_type": "area_of_interest"
                      }
                  },
                  {
                    "range": {
                      "starttime": {
                        "lte": endtime
                      }
                    }
                  }
                ]
              }
            },
            "filter": {
              "missing": {
                "field": "endtime"
              }
            }
          }
        },
        {
        "filtered": {
            "query": {

              "bool": {
                "must": [
                  {
                    "match": {
                      "dataset_type": "area_of_interest"
                      }
                  },
                  {
                    "range": {
                      "endtime": {
                        "gte": starttime
                      }
                    }
                  }
                ]
              }
            },
            "filter": {
              "missing": {
                "field": "starttime"
              }
            }
          }
        }
          
      ]
    }
  },
        "partial_fields" : {
            "partial" : {
                "include" : [ "id", "starttime", "endtime", "location", 
                              "metadata.user_tags", "metadata.priority" ]
            }
        }
    }

    # filter inactive
    hits = [i['fields']['partial'][0] for i in query_es(query) 
            if 'inactive' not in i['fields']['partial'][0].get('metadata', {}).get('user_tags', [])]
    #logger.info("hits: {}".format(json.dumps(hits, indent=2)))
    #logger.info("aois: {}".format(json.dumps([i['id'] for i in hits])))
    return hits

def get_orbit_file(orbit_dt, platform):
    logger.info("get_orbit_file : %s : %s" %(orbit_dt, platform))
    hits = util.query_orbit_file(orbit_dt, orbit_dt, platform)
    #logger.info("get_orbit_file : hits : \n%s\n" %hits)
    logger.info("get_orbit_file returns %s result " %len(hits))
    #return hits

    
    for hit in hits:
        metadata = hit["metadata"]     
        id = hit['id']     
        orbit_platform = metadata["platform"]   
        logger.info(orbit_platform)
        if orbit_platform == platform:
            url = metadata["context"]["localize_urls"][0]["url"]
            return True, id, url
    return False, None, None   
   


def query_aois_new(starttime, endtime):
    """Query ES for active AOIs that intersect starttime and endtime."""

    es_index = "grq_*_area_of_interest"
    query = {
        "query": {
            "bool": {
                "should": [
                    {
                        "bool": {
                            "must": [
                                {
                                    "range": {
                                        "starttime": {
                                            "lte": endtime
                                        }
                                    }
                                },
                                {
                                    "range": {
                                        "endtime": {
                                            "gte": starttime
                                        }
                                    }
                                },
				{
                                    "match": {
					    "dataset_type": "area_of_interest"
                                        }
                                },
                                {
                                    "match": {
                                            "metadata.tags": "standard_product"
                                        }
                                }
                            ],
                 	    "must_not": {
                		"term": {
                    		    "metadata.user_tags": "inactive"
                		}
            		    }
			  
                        }
                    },
        {
        "filtered": {
            "query": {

              "bool": {
                "must": [
                  {
                    "match": {
                      "dataset_type": "area_of_interest"
                      }
                  },
                  {
                    "range": {
                      "starttime": {
                        "lte": endtime
                      }
                    }
                  },
                  {
                      "match": {
                          "metadata.user_tags": "standard_product"
                      }
                  }
                ],
       	    	"must_not": {
                    "term": {
                        "metadata.user_tags": "inactive"
                    }
                }
              }
            },
            "filter": {
              "missing": {
                "field": "endtime"
              }
            }
          }
        },
        {
        "filtered": {
            "query": {

              "bool": {
                "must": [
                  {
                    "match": {
                      "dataset_type": "area_of_interest"
                      }
                  },
                  {
                    "range": {
                      "endtime": {
                        "gte": starttime
                      }
                     }
                  },
                  {
                      "match": {
                          "metadata.user_tags": "standard_product"
                      }
                  }
                ],
           	"must_not": {
                    "term": {
                        "metadata.user_tags": "inactive"
                    }
                }
              }
            },
            "filter": {
              "missing": {
                "field": "starttime"
              }
            }
          }
        }
          
      ]
    }
  },
        "partial_fields" : {
            "partial" : {
                "include" : [ "id", "starttime", "endtime", "location", 
                              "metadata.user_tags", "metadata.priority" ]
            }
        }
    }

    # filter inactive
    hits = [i['fields']['partial'][0] for i in query_es(query) 
            if 'inactive' not in i['fields']['partial'][0].get('metadata', {}).get('user_tags', [])]
    #logger.info("hits: {}".format(json.dumps(hits, indent=2)))
    #logger.info("aois: {}".format(json.dumps([i['id'] for i in hits])))
    return hits

def get_aois_by_id(aoi_list):
    aois = []
    for aoi in aoi_list:
        aoi_data = get_aoi_data_by_id(aoi)
        logger.info("aoi_data : %s" %aoi_data)
        if aoi_data and len(aoi_data)>0:
            logger.info("Adding data for aoi: %s" %aoi)
            aois.extend(aoi_data)
        else:
            logger.info("No data found for aoi: %s" %aoi)
    return aois


def get_aoi_data_by_id(aoi_id):
    es_index = "grq_*_area_of_interest"
    # query
    query = {
        "query":{
            "bool":{
                "must":[
                    { "term":{ "_id": aoi_id } },
                ]
            }
        },
         "partial_fields" : {
            "partial" : {
                "include" : [ "id", "starttime", "endtime", "location" ]
            }
        }
    }
     # filter inactive
    hits = [i['fields']['partial'][0] for i in query_es(query)]
    logger.info("hits: {}".format(json.dumps(hits, indent=2)))
    #logger.info("aois: {}".format(json.dumps([i['id'] for i in hits])))
    return hits


def get_dem_type(acq):
    dem_type = "SRTM+v3"
    if acq['city'] is not None and len(acq['city'])>0:
        if acq['city'][0]['country_name'] is not None and acq['city'][0]['country_name'].lower() == "united states":
            dem_type="Ned1"
    return dem_type

def getUpdatedTime(s, m):
    #date = dateutil.parser.parse(s, ignoretz=True)
    #new_date = s + timedelta(minutes = m)
    new_date = s + timedelta(minutes = m)
    return new_date

def get_time(t):

    logger.info("get_time(t) : %s" %t)
    t = parser.parse(t).strftime('%Y-%m-%dT%H:%M:%S')
    t1 = datetime.strptime(t, '%Y-%m-%dT%H:%M:%S')
    logger.info("returning : %s" %t1)
    return t1


def isTrackSelected(land, water, land_area, water_area):
    selected = False
    total_acq_land = 0

    for acq_land in land:
        total_acq_land+= acq_land

    if ((total_acq_land*100)/land)> 98:
        selected = True

    return selected


def update_dateformat(d):
    logger.info("update_dateformat in: %s" %d)
    try:
        if isinstance(d, datetime):
            d = d.strftime('%Y-%m-%dT%H:%M:%SZ')
        elif isinstance(d, str):
            d = parser.parse(d).strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            logger.info("unknown type : %s" %type(d))
    except Exception as err:
        logger.info(str(err))
        traceback.print_exc()
    logger.info("update_dateformat out: %s" %d)
    return d
def update_dateformat2(d):
    logger.info("update_dateformat in: %s" %d)
    try:
        if isinstance(d, datetime):
            d = d.strftime('%Y%m%dT%H%M%S')
        elif isinstance(d, str):
            d = parser.parse(d).strftime('%Y%m%dT%H%M%S')
        else:
            logger.info("unknown type : %s" %type(d))
    except Exception as err:
        logger.info(str(err))
        traceback.print_exc()
    logger.info("update_dateformat out: %s" %d)
    return d

def write_result_file(result_file, result):
    try:

        with open(result_file, 'a') as fo:
            cw = csv.writer(fo, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            cw.writerow(["Date", "Orbit", "Type", "Track","Track_Land","Total_Acquisition_Land", "area_delta_in_resolution", "area_threshold_passed", "Orbit_Quality_Test_Passed", "Reference_Unique_IPF_Count", "Secondary_Unique_IPF_Count",  "BlackList_Test_Passed", "Enumeration_Passed", "Candidate_Pairs", "Failure_Reason", "comment","Track_AOI_Intersection", "ACQ_POEORB_AOI_Intersection"])

            cw.writerow([result.get('dt', ''), result.get('orbit_name', ''), "Primary", result.get('track', ''),result.get('Track_POEORB_Land', '') , result.get('ACQ_Union_POEORB_Land', ''), result.get('res', ''), result.get('area_threshold_passed', ''), result.get('WATER_MASK_PASSED', ''), result.get('primary_ipf_count', ''), result.get('secondary_ipf_count', ''), result.get('BL_PASSED', ''), result.get('matched', ''), result.get('candidate_pairs', ''), result.get('fail_reason', ''), result.get('comment', ''), result.get('Track_AOI_Intersection', ''), result.get('ACQ_POEORB_AOI_Intersection', '')])

    except Exception as err:
        logger.info("Error writing to csv file : %s : " %str(err))
        traceback.print_exc()


    
def publish_result(reference_result, id_hash):
  
    version = "v2.0.0"
    logger.info("\nPUBLISH RESULT")
    #write_result_file(result_file, reference_result)


    orbit_type = 'poeorb'
    aoi_id = reference_result['aoi'].strip().replace(' ', '_')
    logger.info("aoi_id : %s" %aoi_id)
    reference_result['list_slave_dt']="00000000T000000"
    
    ACQ_RESULT_ID_TMPL = "S1-GUNW-acqlist-audit_trail-R{}-M{:d}S{:d}-TN{:03d}-{}-{}-{}-{}"
    id = ACQ_RESULT_ID_TMPL.format('M', reference_result.get('master_count', 0), reference_result.get('slave_count', 0), reference_result.get('track', 0), update_dateformat2(reference_result.get('list_master_dt', '')), update_dateformat2(reference_result.get('list_slave_dt', '')), orbit_type, id_hash[0:4])



    logger.info("publish_result : id : %s " %id)
    #id = "acq-list-%s" %id_hash[0:4]
    prod_dir =  id
    os.makedirs(prod_dir, 0o755)

    met_file = os.path.join(prod_dir, "{}.met.json".format(id))
    ds_file = os.path.join(prod_dir, "{}.dataset.json".format(id))
    
    aoi = []
    track = []
    full_id_hash = reference_result.get('full_id_hash', None)
    this_aoi =  reference_result.get('aoi', None)
    if this_aoi:
        aoi.append(this_aoi)

    this_track = reference_result.get('track', None)
    if this_track:
        track.append(this_track)

    if full_id_hash:
        track, aoi = util.get_complete_track_aoi_by_hash(full_id_hash, track, aoi)

    logger.info("publish_result : Final AOI : {}, Final Track : {}".format(aoi, track))


    logger.info("\n\npublish_result: PUBLISHING %s : " %id)  
    #with open(met_file) as f: md = json.load(f)
    md = {}
    md['id'] = id
    md['aoi'] =  aoi
    md['reference_orbit'] = reference_result.get('orbit_name', '')
    md['reference_orbit_quality_passed'] = reference_result.get('orbit_quality_check_passed', '')
    md['reference_tract_land'] = reference_result.get('Track_POEORB_Land', '')
    md['reference_total_acqusition_land'] = reference_result.get('ACQ_Union_POEORB_Land', '')
    md['pair_created'] = reference_result.get('result', '')
    md['track_number'] = track
    md['failure_reason'] = reference_result.get('fail_reason', '')
    md['comment'] = reference_result.get('comment', '')
    md['starttime'] = update_dateformat(reference_result.get('starttime', ''))
    md['endtime'] = update_dateformat(reference_result.get('endtime', ''))
    md['reference_area_threshold_passed'] = reference_result.get('area_threshold_passed', '')
    md['reference_date'] = update_dateformat(reference_result.get('dt', ''))
    md['reference_delta_area_sqkm'] = reference_result.get('delta_area', '')
    md['reference_delta_area_pixel'] = reference_result.get('res', '')
    md['union_geojson'] = reference_result.get('union_geojson', '')
    md['reference_dropped_ids']=reference_result.get('master_dropped_ids', [])
    md['full_id_hash']=reference_result.get('full_id_hash', '')
    md['reference_acquisitions'] = reference_result.get('master_acquisitions', [])
    md['secondary_acquisitions'] = reference_result.get('slave_acquisitions', [])
    md['reference_scenes'] = reference_result.get('master_scenes', [])
    md['secondary_scenes'] = reference_result.get('slave_scenes', [])
    md['secondary_date'] = update_dateformat(reference_result.get('dt', ''))
    md['failed_orbit'] = reference_result.get('failed_orbit', '')
    md['tags'] =  reference_result.get('tags', [])

    with open(met_file, 'w') as f: json.dump(md, f, indent=2)

    logger.info("publish_result : creating dataset file : %s" %ds_file)
    util.create_dataset_json(id, version, met_file, ds_file)

        
def print_groups(grouped_matched):
    for track in grouped_matched["grouped"]:
        logger.info("\nTrack : %s" %track)
        for day_dt in sorted(grouped_matched["grouped"][track], reverse=True):
            logger.info("\tDate : %s" %day_dt)
            for acq in grouped_matched["grouped"][track][day_dt]:

                logger.info("\t\t %s" %acq[0])


def group_acqs_by_track_date_from_metadata(frames):
    logger.info("group_acqs_by_track_date_from_metadata")
    return util.group_acqs_by_track_multi_date(create_acqs_from_metadata(frames))


def create_acqs_from_metadata(frames):
    acqs = []
    logger.info("frame length : %s" %len(frames))
    for acq in frames:
        logger.info("create_acqs_from_metadata : %s" %acq['id'])
        acq_obj = util.create_acq_obj_from_metadata(acq)
        if acq_obj:
            acqs.append(acq_obj)
    return acqs

def get_covered_acquisitions_by_track_date(acqs, threshold_pixel, orbit_file, orbit_dir, platform, result_file, selected_track_list, location_geojson, tag_list, aoi_id = None):
    #util.print_acquisitions(aoi['id'], util.create_acqs_from_metadata(acqs))

    if not aoi_id:
        aoi_id = DEFAULT_AOI

    logger.info("\nget_covered_acquisitions_by_track_date")
    #logger.info(acqs) 
    grouped_matched = util.group_acqs_by_track_date_from_metadata(acqs) #group_acqs_by_track(acqs)
    logger.info("grouped_matched Done")
    print_groups(grouped_matched)
    matched_ids = list(grouped_matched["acq_info"].keys())

    #logger.info("grouped_matched : %s" %grouped_matched)
    logger.info("matched_ids : %s" %matched_ids)
    logger.info("PLATFORM : %s" %platform)
    orbit_type = "P"
    orbit_file = os.path.basename(orbit_file)
    mission = "S1A"
    if platform == "Sentinel-1B":
        mission = "S1B"

   
    selected_track_acqs = {}
    result_track_acqs = {}
  
    logger.info("Tracks to process : %s" %grouped_matched["grouped"])
    for track in grouped_matched["grouped"]:
        logger.info("get_covered_acquisitions_by_track_date : Processing track : %s" %track)
        if len(selected_track_list)>0:
            if int(track) not in selected_track_list:
                logger.info("%s not in selected_track_list %s. So skipping this track" %(track, selected_track_list))
                continue
        selected_track_dt_acqs = {}
        result_track_dt_acqs = {}
        
        for track_dt in grouped_matched["grouped"][track]:
            filtered_acd_ids, dropped_ids = util.filter_acq_ids(grouped_matched["acq_info"], grouped_matched["grouped"][track][track_dt])
            logger.info("filtered_acd_ids : %s" %filtered_acd_ids)
            valid_orbit = False
            valid_orbit_err = ''

            try:
                selected, result, removed_ids = gtUtil.water_mask_check(track, track_dt, grouped_matched["acq_info"], filtered_acd_ids,  location_geojson, aoi_id, threshold_pixel, mission, orbit_type, orbit_file, orbit_dir)
                valid_orbit = True
                orbit_name = orbit_file.split('.EOF')[0].strip()
                if len(removed_ids)>0:
                    logger.info("Removed Acquisitions by WaterMaskTest : %s" %removed_ids)
                    for acq_id in removed_ids:
                        logger.info("removing %s from filtered_acd_ids" %acq_id)
                                     
                        filtered_acd_ids.remove(acq_id)
                logger.info("filtered_acd_ids : %s:" %filtered_acd_ids)
            except InvalidOrbitException as err:
                selected = False
                valid_orbit = False
                valid_orbit_err = err
            result['orbit_name']= orbit_name
            result['track'] = track
            result['master_dropped_ids'] = dropped_ids
            result_track_dt_acqs[track_dt] = result
            starttime, endtime = util.get_start_end_time2(grouped_matched["acq_info"], filtered_acd_ids)
            result['starttime'] = starttime
            result['endtime'] = endtime
            result['union_geojson']= location_geojson
            #master_dt_str = util.get_time_str_with_format(track_dt, "%Y%m%dT%H%M%S")
            logger.info("master_dt_str : %s" %track_dt)

            result['list_master_dt'] = track_dt
            result['list_slave_dt'] = track_dt
            result['master_count'] = 1
            result['slave_count'] = 0
            result['tags'] = tag_list
 
            if selected:
                #logger.info("SELECTED : aoi : %s track : %s  track_dt : %s" %(aoi['id'], track, track_dt))
                selected_acqs = []
                for acq_id in filtered_acd_ids:
                    acq = grouped_matched["acq_info"][acq_id]

                        #acq.pv = pv #util.get_processing_version(acq.identifier)
                        #util.update_grq(acq_id, acq.pv)
                    logger.info("APPENDING : %s" %acq_id)
                    selected_acqs.append(acq)
                selected_track_dt_acqs[track_dt] = selected_acqs
                result['orbit_quality_check_passed']=True
            else:
                result['result'] = False
                id_hash = '0000'
                result['orbit_quality_check_passed']=False
                result['failed_orbit'] = 'reference'
                publish_result(result, id_hash)
           
            if not valid_orbit:
                 raise InvalidOrbitException(valid_orbit_err) 
            try:
                with open(result_file, 'a') as fo:
                    cw = csv.writer(fo, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                    cw.writerow([result.get('dt', ''), result.get('orbit_name', ''), "Primary", result.get('track', ''),result.get('Track_POEORB_Land', '') , result.get('ACQ_Union_POEORB_Land', ''), result.get('delta_area', ''), result.get('res', ''), result.get('area_threshold_passed', ''), result.get('WATER_MASK_PASSED', ''), result.get('primary_ipf_count', ''), result.get('secondary_ipf_count', ''), result.get('BL_PASSED', ''), result.get('matched', ''), result.get('candidate_pairs', ''), result.get('fail_reason', ''), result.get('comment', ''), result.get('Track_AOI_Intersection', ''), result.get('ACQ_POEORB_AOI_Intersection', ''), tag_list])

            except Exception as err:
                logger.info("\n\nERROR Writing to csv file : %s" %str(err))
                traceback.print_exc()
        selected_track_acqs[track] = selected_track_dt_acqs
        logger.info("CHECK: selected_track_acqs[track] : %s" %selected_track_acqs[track])
        
        result_track_acqs[track] = result_track_dt_acqs


    #exit (0)
    logger.info("get_covered_acquisitions_by_track_date returns : %s" %selected_track_acqs)
    return selected_track_acqs, result_track_acqs
 
def get_covered_acquisitions(aoi, acqs, orbit_file):
    #util.print_acquisitions(aoi['id'], util.create_acqs_from_metadata(acqs))

    logger.info("AOI : %s" %aoi['location'])
    grouped_matched = util.group_acqs_by_orbit_number_from_metadata(acqs) #group_acqs_by_track(acqs)
    matched_ids = list(grouped_matched["acq_info"].keys())
           
    #logger.info("grouped_matched : %s" %grouped_matched)
    logger.info("matched_ids : %s" %matched_ids)


    selected_track_acqs = {}

    

    for track in grouped_matched["grouped"]:
        selected_orbitnumber_acqs = {}
        for orbitnumber in grouped_matched["grouped"][track]:
            selected = gtUtil.water_mask_check(track, orbitnumber, grouped_matched["acq_info"], grouped_matched["grouped"][track][orbitnumber],  aoi['location'], aoi['id'], threshold_pixel, orbit_file)
            if selected:
                logger.info("SELECTED")
                selected_acqs = []
                for pv in grouped_matched["grouped"][track][orbitnumber]:
                    for acq_id in grouped_matched["grouped"][track][orbitnumber][pv]:
                        acq = grouped_matched["acq_info"][acq_id]
                        
                        if not acq.pv:
                            acq.pv = pv #util.get_processing_version(acq.identifier)
                            #util.update_grq(acq_id, acq.pv)
                        logger.info("APPENDING : %s" %acq_id)
                        selected_acqs.append(acq)
                selected_orbitnumber_acqs[orbitnumber] = selected_acqs
        selected_track_acqs[track] = selected_orbitnumber_acqs     

        

    #exit (0)

    return selected_track_acqs

def query_aoi_acquisitions(starttime, endtime, platform, orbit_file, orbit_dir, threshold_pixel, acquisition_version, selected_track_list, selected_aoi_list, tag_list=[]):
    """Query ES for active AOIs that intersect starttime and endtime and 
       find acquisitions that intersect the AOI polygon for the platform."""
    #aoi_acq = {}
    orbit_aoi_data = {}
    es_index = "grq_*_*acquisition*"
    es_index = "grq_%s_acquisition-s1-iw_slc/acquisition-S1-IW_SLC/" %(acquisition_version)
    logger.info("query_aoi_acquisitions : es_index : %s" %es_index)
    aois = None
    if len(selected_aoi_list)>0:
        aois = get_aois_by_id(selected_aoi_list)
    else:
        aois = query_aois_new(starttime, endtime)

    logger.info("No of AOIs : %s " %len(aois))
    logger.info("aois : %s" %aois)

    if not aois or len(aois) <=0:
        logger.info("Existing as NO AOI Found")
        sys.exit(0)
    for aoi in aois:
        logger.info("aoi: {}".format(aoi['id']))
        selected_track_acqs, result_track_acqs = get_location_data(starttime, endtime, platform, orbit_file, orbit_dir, threshold_pixel, acquisition_version, selected_track_list, aoi['location'], tag_list, aoi['id'])
        
        if len(list(selected_track_acqs.keys()))==0:
            logger.info("Nothing selected from AOI %s " %aoi['id'])
            continue
        
        #for acq in acqs:
        aoi_data = {}
        aoi_priority = aoi.get('metadata', {}).get('priority', 0)
        # ensure highest priority is assigned if multiple AOIs resolve the acquisition
        #if acq['id'] in acq_info and acq_info[acq['id']].get('priority', 0) > aoi_priority:
            #continue
        aoi_data['aoi_id'] = aoi['id']
        aoi_data['aoi_location'] =  aoi['location']
        aoi_data['priority'] = aoi_priority
        aoi_data['selected_track_acqs'] = selected_track_acqs
        aoi_data['result_track_acqs'] = result_track_acqs
        orbit_aoi_data[aoi['id']] = aoi_data
        #acq_info[aoi_data['id']] = acq
        #aoi_acq[aoi] = acq_info 
        #logger.info("Acquistions to localize: {}".format(json.dumps(acq_info, indent=2)))

    if len(list(orbit_aoi_data.keys()))<=0:
        logger.info("Existing as NOTHING selected for any aois")
        sys.exit(0)
    return orbit_aoi_data


def query_geojson_acquisitions(starttime, endtime, platform, orbit_file, orbit_dir, threshold_pixel, acquisition_version, selected_track_list, location_geojson, tag_list=[]):
    """Query ES for active AOIs that intersect starttime and endtime and
       find acquisitions that intersect the location geojson for the platform."""
    #aoi_acq = {}
    orbit_aoi_data = {}
    selected_track_acqs = []
    result_track_acqs = []
    aoi_id = DEFAULT_AOI

    try:
        selected_track_acqs, result_track_acqs = get_location_data(starttime, endtime, platform, orbit_file, orbit_dir, threshold_pixel, acquisition_version, selected_track_list, location_geojson, tag_list)

        if len(list(selected_track_acqs.keys()))==0:
            logger.info("Exiting as nothing selected from geojson %s " %location_geojson)
            sys.exit(0)

        #for acq in acqs:
        aoi_data = {}
        aoi_priority = 0 #aoi.get('metadata', {}).get('priority', 0)
        # ensure highest priority is assigned if multiple AOIs resolve the acquisition
        #if acq['id'] in acq_info and acq_info[acq['id']].get('priority', 0) > aoi_priority:
            #continue
        aoi_data['aoi_id'] = aoi_id
        aoi_data['aoi_location'] =  location_geojson
        aoi_data['priority'] = aoi_priority
        aoi_data['selected_track_acqs'] = selected_track_acqs
        aoi_data['result_track_acqs'] = result_track_acqs
        orbit_aoi_data[aoi_id] = aoi_data
        #acq_info[aoi_data['id']] = acq
        #aoi_acq[aoi] = acq_info
        #logger.info("Acquistions to localize: {}".format(json.dumps(acq_info, indent=2)))
    except Exception as err:
        logger.info("Error in query_geojson_acquisition : {}".format(str(err)))
        traceback.print_exc()

    if len(list(orbit_aoi_data.keys()))<=0:
        logger.info("Existing as NOTHING selected for any aois")
        sys.exit(0)
    return orbit_aoi_data

def get_location_data(starttime, endtime, platform, orbit_file, orbit_dir, threshold_pixel, acquisition_version, selected_track_list, location_geojson, tag_list=[], aoi_id = None):
 
    selected_track_acqs = {}
    result_track_acqs = {}
    if not aoi_id:
        aoi_id = DEFAULT_AOI

    try:
        es_index = "grq_*_*acquisition*"
        es_index = "grq_%s_acquisition-s1-iw_slc/acquisition-S1-IW_SLC/" %(acquisition_version)
        logger.info("query_aoi_acquisitions : es_index : %s" %es_index)

        query = {
            "query": {
                "filtered": {
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "term": {
                                        "dataset_type.raw": "acquisition"
                                    }
                                },
                                 {
                                    "term": {
                                        "version.raw": acquisition_version
                                    }
                                },
                                {
                                    "term": {
                                        "metadata.platform.raw": platform
                                    }
                                },
                                {
                                    "range": {
                                        "starttime": {
                                            "lte": endtime
                                        }
                                    }
                                },
                                {
                                    "range": {
                                        "endtime": {
                                            "gte": starttime
                                        }
                                    }
                                }
                            ],
                            "must_not": {
                                "term": {
                                    "metadata.tags": "deprecated"
                                }
                            }
                        }
                    },
                    "filter": {
                        "geo_shape": {  
                            "location": {
                                "shape": location_geojson
                            }
                        }
                    }
                }
            },
            "partial_fields" : {
                "partial" : {
                    "include" : [ "id", "dataset_type", "dataset", "metadata", "city", "continent", "starttime", "endtime"]
                }
            }
        }
        logger.info(json.dumps(query, indent=4))
        acqs = [i['fields']['partial'][0] for i in query_es(query, es_index)]
        logger.info("Found {} acqs for {}: {}".format(len(acqs), aoi_id,
                    json.dumps([i['id'] for i in acqs], indent=2)))

        #logger.info("ALL ACQ of AOI : \n%s" %acqs)
        if len(acqs) <=0:
            logger.info("Excluding location %s as no acquisitions there" %location_geojson)
        selected_track_acqs = {}

        if not aoi_id:
            aoi_id = DEFAULT_AOI

        result_file = "RESULT_SUMMARY_%s.csv" %aoi_id
        with open(result_file, 'w') as fo:
            cw = csv.writer(fo, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            cw.writerow(["Date", "Orbit", "Type", "Track","Track_Land","Total_Acquisition_Land", "delta_area_sqkm", "delta_area_pixel", "area_threshold_passed", "Orbit_Quality_Test_Passed", "Reference_Unique_IPF_Count", "Secondary_Unique_IPF_Count",  "BlackList_Test_Passed", "Enumeration_Passed", "Candidate_Pairs", "Failure_Reason", "comment","Track_AOI_Intersection", "ACQ_POEORB_AOI_Intersection", "Machine_Tags"])

        selected_track_acqs, result_track_acqs = get_covered_acquisitions_by_track_date(acqs, threshold_pixel, orbit_file, orbit_dir, platform, result_file, selected_track_list, location_geojson, tag_list, aoi_id)
    except Exception as err:
        logger.info("Error in get_location_data : {}".format(str(err)))
        traceback.print_exc()
    return selected_track_acqs, result_track_acqs

def resolve_s1_slc(identifier, download_url, project):
    """Resolve S1 SLC using ASF datapool (ASF or NGAP). Fallback to ESA."""

    # determine best url and corresponding queue
    vertex_url = "https://datapool.asf.alaska.edu/SLC/SA/{}.zip".format(identifier)
    r = requests.head(vertex_url, allow_redirects=True)
    if r.status_code == 403:
        url = r.url
        queue = "{}-job_worker-small".format(project)
    elif r.status_code == 404:
        url = download_url
        queue = "factotum-job_worker-scihub_throttled"
    else:
        raise RuntimeError("Got status code {} from {}: {}".format(r.status_code, vertex_url, r.url))
    return url, queue


class DatasetExists(Exception):
    """Exception class for existing dataset."""
    pass

def get_temporal_baseline(ctx):
    temporalBaseline = 24
    if 'temporalBaseline' in ctx:
        temporalBaseline = int(ctx['temporalBaseline'])
    return temporalBaseline
   

def resolve_s1_slc(identifier, download_url, project):
    """Resolve S1 SLC using ASF datapool (ASF or NGAP). Fallback to ESA."""

    # determine best url and corresponding queue
    vertex_url = "https://datapool.asf.alaska.edu/SLC/SA/{}.zip".format(identifier)
    r = requests.head(vertex_url, allow_redirects=True)
    if r.status_code == 403:
        url = r.url
        queue = "{}-job_worker-small".format(project)
    elif r.status_code == 404:
        url = download_url
        queue = "factotum-job_worker-scihub_throttled"
    else:
        raise RuntimeError("Got status code {} from {}: {}".format(r.status_code, vertex_url, r.url))
    return url, queue


class DatasetExists(Exception):
    """Exception class for existing dataset."""
    pass

def get_temporal_baseline(ctx):
    temporalBaseline = 24
    if 'temporalBaseline' in ctx:
        temporalBaseline = int(ctx['temporalBaseline'])
    return temporalBaseline

def resolve_acqs(ctx_file):
    """Resolve best URL from acquisitions from AOIs."""

    # read in context
    with open(ctx_file) as f:
        ctx = json.load(f)

    project = 'grfn'
    logger.info("PROJECT : %s" %project)
    priority = int(ctx["job_priority"])
    minMatch = int(ctx["minMatch"])
    dataset_version = ctx["dataset_version"] 
    acquisition_version = ctx["acquisition_version"]  
    threshold_pixel = int(ctx["threshold_pixel"])
    job_type, job_version = ctx['job_specification']['id'].split(':')
    skip_days = int(ctx.get("skipDays", 0))
    selected_track_list = []
    tag_list = []
    if "machine_tags" in ctx and ctx["machine_tags"] is not None:
        tag_list = ctx["machine_tags"].strip().split(',')
    

    try:
        if "track_numbers" in ctx and ctx["track_numbers"] is not None:
            track_numbers = ctx["track_numbers"].strip()
            if track_numbers:
                track_numbers_list = track_numbers.split(',')
                for tn in track_numbers_list:
                    selected_track_list.append(int(tn))
    except:
        pass
    selected_aoi_list = []

    try:
        if "aoi_name" in ctx and ctx["aoi_name"] is not None:
            aois = ctx["aoi_name"].strip()
            logger.info("passed aoi: %s" %aois)
            if aois:
                aoi_list = aois.split(',')
                logger.info(aoi_list)
                for aoi in aoi_list:
                    selected_aoi_list.append(aoi.strip())
    except:
        pass
    selected_aoi_list = list(set(selected_aoi_list))
    logger.info("selected_aoi_list : %s" %selected_aoi_list)
    logger.info("selected_track_list : %s" %selected_track_list)
    logger.info("skip_days : %s" %skip_days)

    #Find Orbit File Info
    orbit_file = None
    orbit_file_dir =os.path.basename(ctx["localize_urls"][0]["url"])
    for file in os.listdir(orbit_file_dir):
        if file.endswith(".EOF"):
            orbit_file = os.path.join(orbit_file_dir, file)

    if not orbit_file:
        raise RuntimeError("Orbit File NOT Found")
    else:
        logger.info("Orbit File : %s " %orbit_file)

    orbit_aoi_data = None
    
    if "aoi_name" in ctx:
        orbit_aoi_data = query_aoi_acquisitions(ctx['starttime'], ctx['endtime'], ctx['platform'], orbit_file, orbit_file_dir, threshold_pixel, acquisition_version, selected_track_list, selected_aoi_list, tag_list)
    elif "location_geojson":
        location_geojson =  json.loads(ctx["location_geojson"])
        logger.info("location_geojson : {}".format(location_geojson))
        '''
        try:
            json.loads(location_geojson)
        except Exception as err:
            err_msg = "Invalid input geojson : {} : {}".format(str(err), location_geojson)
            logger.info(err_msg)
            raise Exception(err_msg)
        '''
        orbit_aoi_data = query_geojson_acquisitions(ctx['starttime'], ctx['endtime'], ctx['platform'], orbit_file, orbit_file_dir, threshold_pixel, acquisition_version, selected_track_list, location_geojson, tag_list)
    #osaka.main.get("http://aux.sentinel1.eo.esa.int/POEORB/2018/09/15/S1A_OPER_AUX_POEORB_OPOD_20180915T120754_V20180825T225942_20180827T005942.EOF")
    #logger.info(orbit_aoi_data)
    #exit(0)
    
    # build args
    #queue = ctx["recommended-queues"][0]
    queue = "system-jobs-queue"
    singlesceneOnly = True
    precise_orbit_only = True
    
    job_data = {}
    job_data["project"] = project
    '''
    job_data["spyddder_extract_version"] = spyddder_extract_version
    job_data["standard_product_ifg_version"] = standard_product_ifg_version
    job_data["acquisition_localizer_version"] = acquisition_localizer_version
    job_data["standard_product_localizer_version"] = standard_product_localizer_version
    '''

    job_data["job_type"] = job_type
    job_data["job_version"] = job_version
    job_data["job_priority"] = ctx['job_priority']
    job_data['orbit_file'] = orbit_file 
    job_data['minMatch'] = minMatch
    job_data['threshold_pixel'] = threshold_pixel
    job_data["acquisition_version"] = acquisition_version
    job_data["selected_track_list"] = selected_track_list
    job_data["skip_days"] = skip_days
    
    orbit_data = {}
    orbit_data['starttime'] = ctx['starttime']
    orbit_data['endtime'] = ctx['endtime']
    orbit_data['platform'] = ctx['platform']
    orbit_data['orbit_file'] = orbit_file  

    orbit_acq_selections = {}
    orbit_acq_selections["job_data"] = job_data
    orbit_acq_selections["orbit_aoi_data"] = orbit_aoi_data
    orbit_acq_selections["orbit_data"] = orbit_data
    orbit_acq_selections["tag_list"] = tag_list 

    return orbit_acq_selections



def main():

    # read in _context.json
    context_file = os.path.abspath("_context.json")
    if not os.path.exists(context_file):
        raise RuntimeError("Context file doesn't exist.")
    
    resolve_acqs(context_file)

if __name__ == "__main__":
    sys.exit(main())
