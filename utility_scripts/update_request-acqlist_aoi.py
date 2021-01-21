#!/usr/bin/env python
"""
Search for failed jobs with osaka no-clobber errors during dataset publishing
and clean them out of S3 if the dataset was not indexed.
"""
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from builtins import range
from future import standard_library
standard_library.install_aliases()
import os
import sys
import re
import requests
import json
import logging
import argparse
import boto3
import types
import hashlib
import elasticsearch

from hysds.celery import app


log_format = "[%(asctime)s: %(levelname)s/clean_failed_s3_no_clobber_datasets] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

def appennd_audit_trail_request_aoi(es_url, id, aoi):
    try:
        es_index = "grq_*_runconfig-acqlist-audit_trail"
        _type = "runconfig-acqlist-audit_trail"

        ES = elasticsearch.Elasticsearch(es_url)
        ES.update(index=es_index, doc_type=_type, id=id,
              body={"doc": {"metadata": {"aoi": aoi}}}) 
        print("Updated  audit trail : %s AOI :  %s successfully" %(id, aoi ))    
    except Exception as err:
        print("ERROR : updationg AOI : %s" %str(err))

def remove_local(s, suffix):
    if suffix and s.endswith(suffix):
        return s[:-len(suffix)]
    return s

def query_es(query, rest_url, es_index):
    """Query ES."""
    url = "{}/{}/_search?search_type=scan&scroll=60&size=100".format(rest_url, es_index)
    #print("url: {}".format(url))
    r = requests.post(url, data=json.dumps(query))

    if r.status_code != 200:
        print("Failed to query %s:\n%s" % (es_url, r.text))
        print("query: %s" % json.dumps(query, indent=2))
        print("returned: %s" % r.text)
        r.raise_for_status()

    #r.raise_for_status()
    scan_result = r.json()
    #print("scan_result: {}".format(json.dumps(scan_result, indent=2)))
    count = scan_result['hits']['total']
    if '_scroll_id' not in scan_result:
        print("_scroll_id not found in scan_result. Returning empty array for the query :\n%s" %query)
        return []

    scroll_id = scan_result['_scroll_id']
    hits = []
    while True:
        r = requests.post('%s/_search/scroll?scroll=60m' % rest_url, data=scroll_id)
        if r.status_code != 200:
            print("Failed to query %s:\n%s" % (es_url, r.text))
            print("query: %s" % json.dumps(query, indent=2))
            print("returned: %s" % r.text)
            r.raise_for_status()

        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        hits.extend(res['hits']['hits'])
    return hits

def get_ifg_hash(master_slcs,  slave_slcs):

    master_ids_str=""
    slave_ids_str=""

    for slc in sorted(master_slcs):
        #print("get_ifg_hash : master slc : %s" %slc)
        if isinstance(slc, tuple) or isinstance(slc, list):
            slc = slc[0]

        slc = remove_local(slc, "-local")
        if master_ids_str=="":
            master_ids_str= slc
        else:
            master_ids_str += " "+slc

    for slc in sorted(slave_slcs):
        #print("get_ifg_hash: slave slc : %s" %slc)
        if isinstance(slc, tuple) or isinstance(slc, list):
            slc = slc[0]

        slc = remove_local(slc, "-local")
        if slave_ids_str=="":
            slave_ids_str= slc
        else:
            slave_ids_str += " "+slc

    id_hash = hashlib.md5(json.dumps([
            master_ids_str,
            slave_ids_str
            ]).encode("utf8")).hexdigest()
    return id_hash


def update_AOI(grq_es_url, hit, aoi_list):
    request_submit_id = hit["_id"]

    met = hit['_source']['metadata']
    machine_tag = met["tags"]
    request_id = get_request_id_from_machine_tag(machine_tag)
    aoi_request_id = request_id.replace("-", "").replace("_", "").replace(":", "").replace(".", "")
    aoi ="{}{}".format("AOI_ondemand_AOI", aoi_request_id)

    
    print("{} : {} : {}".format(request_submit_id, request_id, aoi))

    if aoi not in aoi_list:
        print("{} NOT in list".format(aoi))

    audit_trail_list = get_datasets_by_request_id(request_id, "runconfig-acqlist-audit_trail", "grq_*_runconfig-acqlist-audit_trail")
    for audit_trail in audit_trail_list:
        print(audit_trail["_id"])
        met = audit_trail['_source']['metadata']
        machine_tag = met["tags"]
        exist_aoi = met["aoi"]
        failure_reason = met["failure_reason"].strip()
        if not failure_reason:
            print("{} : {} : {} : PASSED".format(audit_trail["_id"], exist_aoi, failure_reason))
        else:
            print("{} : {} : {} : FAILED".format(audit_trail["_id"], exist_aoi, failure_reason))
        if aoi not in exist_aoi:
            exist_aoi.append(aoi)
            

def get_datasets_by_request_id(request_id, dataset_type, es_index="grq", rest_url=app.conf['GRQ_ES_URL']):
    """Return all acq-list datasets that contain the acquisition ID."""

    print(es_index)
    query = {
        "query": {
            "bool": {
                "must": [
                  {
                    "term": {
                      "dataset.raw": dataset_type
                    }
                  },
                {
                  "term": {
                    "metadata.tags.raw": request_id
                  }
                }
              ]


            }
        }
    }
    result = query_es(query, rest_url, es_index)
    '''
    if len(result) == 0:
        logger.info("Couldn't find {} containing Request ID: {}".format(dataset_type, request_id))
        sys.exit(0)

    return [i['fields']['partial'][0] for i in result]
    '''
    return result

def get_request_id_from_machine_tag(machine_tag):
    if isinstance(machine_tag, str):
        machine_tag = machine_tag.strip().split(',')
    return get_request_id(machine_tag)

def get_request_id(tag_list):
    request_id = None
    for tag in tag_list:
        if tag.startswith("request"):
            request_id = tag.strip()
            break
    return request_id

def get_tops_ifg(rest_url):
    
    es_index = "grq_v2.0.3_s1-gunw"
    url = "{}/{}/_search?search_type=scan&scroll=60&size=100".format(rest_url, es_index)
    query = {
      "query": {
        "bool": {
          "must": [
            {
              "term": {
                "_type": "S1-GUNW"
              }
            }
          ]
        }
      }
    }


    print(query)
    hits = query_es(query, rest_url, es_index)
    #print(json.dumps(hits[0],indent =4))
    #hits = [i['fields']['partial'][0] for i in query_es(query, rest_url, es_index)]
    print("count : {}".format(len(hits))) 

    return hits

def get_dataset(rest_url, dataset_type, es_index = "grq", version="*"):

    url = "{}/{}/_search?search_type=scan&scroll=60&size=100".format(rest_url, es_index)
    query = {
      "query": {
        "bool": {
          "must": [
            {
              "term": {
                "_type": dataset_type
              }
            }
          ]
        }
      }
    }


    print(query)
    hits = query_es(query, rest_url, es_index)
    #print(json.dumps(hits[0],indent =4))
    #hits = [i['fields']['partial'][0] for i in query_es(query, rest_url, es_index)]
    print("count : {}".format(len(hits)))

    return hits


if __name__ == "__main__":
    grq_es_url=app.conf['GRQ_ES_URL']
    print(grq_es_url)
   
    aoi_list = []
    aois = get_dataset(grq_es_url, "area_of_interest", "grq_v3.0_area_of_interest")
    for aoi in aois:
        if aoi["_id"].startswith("AOI_ondemand_AOI"):
            aoi_list.append(aoi["_id"])
    print(aoi_list)
    hits = get_dataset(grq_es_url, "request-submit", "grq_*_request-submit")
    for hit in hits:
        if hit["_id"].startswith("request-s1gunw-submitted"):
            update_AOI(grq_es_url, hit, aoi_list)
        



