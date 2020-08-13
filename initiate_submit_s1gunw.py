#!/usr/bin/env python

from builtins import str
import os
import sys
import time
import json
import requests
import logging
import traceback
import shutil
import backoff
import util

from hysds.celery import app
from hysds.dataset_ingest import ingest

from request_localizer import query_es, publish_topsapp_runconfig_data, publish_ifgcfg_data, get_acq_object, process_acqlist_localization


# set logger
log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'):
            record.id = '--'
        return True


logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=8, max_value=32)
def query_es(query, idx, url=app.conf['GRQ_ES_URL']):
    """Query ES index."""

    hits = []
    url = url[:-1] if url.endswith('/') else url
    query_url = "{}/{}/_search?search_type=scan&scroll=60&size=100".format(url, idx)
    logger.info("url: {}".format(url))
    logger.info("idx: {}".format(idx))
    logger.info("query: {}".format(json.dumps(query, indent=2)))
    r = requests.post(query_url, data=json.dumps(query))
    r.raise_for_status()
    scan_result = r.json()
    count = scan_result['hits']['total']
    if count == 0: return hits
    scroll_id = scan_result['_scroll_id']
    while True:
        r = requests.post('%s/_search/scroll?scroll=60m' % url, data=scroll_id)
        res = r.json()
        scroll_id = res['_scroll_id']
        if len(res['hits']['hits']) == 0: break
        hits.extend(res['hits']['hits'])
    return hits


def resolve_acq(slc_id, version):
    """Resolve acquisition id."""

    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"metadata.identifier.raw": slc_id}},
                    {"term": {"system_version.raw": version}},
                ]
            }
        },
        "fields": [],
    }
    es_index = "grq_{}_acquisition-s1-iw_slc".format(version)
    result = query_es(query, es_index)

    if len(result) == 0:
        logger.info("query : \n%s\n" % query)
        raise RuntimeError("Failed to resolve acquisition for SLC ID: {} and version: {}".format(slc_id, version))

    return result[0]['_id']


def all_slcs_exist(acq_ids, acq_version, slc_version):
    """Check that SLCs exist for the acquisitions."""

    acq_query = {
        "query": {
            "ids": {
                "values": acq_ids,
            }
        },
        "fields": [
            "metadata.identifier"
        ]
    }
    acq_index = "grq_{}_acquisition-s1-iw_slc".format(acq_version)
    result = query_es(acq_query, acq_index)

    if len(result) == 0:
        error_string = "Failed to resolve all SLC IDs for acquisition IDs: {}".format(acq_ids)
        logger.error(error_string)
        raise RuntimeError(error_string)

    # { < acq_id >: < slc_id >, ...}
    acq_slc_mapper = {row['_id']: row['fields']['metadata.identifier'][0] for row in result}
    slc_ids = [row['fields']['metadata.identifier'][0] for row in result]  # extract slc ids

    if len(acq_ids) != len(acq_slc_mapper):
        for acq_id in acq_ids:
            if not acq_slc_mapper.get(acq_id):
                acq_slc_mapper[acq_id] = None
        acq_slc_mapper_json = json.dumps(acq_slc_mapper, indent=2)
        error_string = "Failed to resolve SLC IDs given the acquisition IDs: \n{}".format(acq_slc_mapper_json)
        logger.error(error_string)
        raise RuntimeError(error_string)

    # check all slc ids exist
    slc_query = {
        "query": {
            "ids": {
                "values": slc_ids,
            }
        },
        "fields": []
    }
    slc_index = "grq_{}_s1-iw_slc".format(slc_version)
    result = query_es(slc_query, slc_index)

    # extract slc ids that exist
    existing_slc_ids = []
    if len(result) > 0:
        for hit in result:
            existing_slc_ids.append(hit['_id'])
    logger.info("slc_ids: {}".format(slc_ids))
    logger.info("existing_slc_ids: {}".format(existing_slc_ids))
    if len(slc_ids) != len(existing_slc_ids):
        logger.info("Missing SLC IDs: {}".format(list(set(slc_ids) - set(existing_slc_ids))))
        return False
    return True


def get_acqlists_by_request_id(request_id, acqlist_version):
    """Return all acq-list datasets that contain the acquisition ID."""

    query = {
        "query": {
            "bool": {
                "must": [
                  {
                    "term": {
                      "dataset.raw": "runconfig-acqlist"
                    }
                  },
                {
                  "term": {
                    "metadata.tags.raw": request_id
                  }
                }
              ]

  
            }
        },
        "partial_fields": {
            "partial": {
                "exclude": ["city", "context", "continent"],
            }
        }
    }
    es_index = "grq_{}_s1-gunw-runconfig-acq-list".format(acqlist_version)
    result = query_es(query, es_index)

    if len(result) == 0:
        logger.info("Couldn't find acq-list containing Request ID: {}".format(request_id))
        sys.exit(0)

    return [i['fields']['partial'][0] for i in result]


def ifgcfg_exists(ifgcfg_id, version):
    """Return True if ifg-cfg exists."""

    query = {
        "query": {
            "ids": {
                "values": [ifgcfg_id],
            }
        },
        "fields": []
    }
    index = "grq_{}_s1-gunw-ifg-cfg".format(version)
    result = query_es(query, index)
    return False if len(result) == 0 else True

def output_dataset_exists(output_dataset_id, version, index = "grq"):
    """Return True if ifg-cfg exists."""
        
    query = {
        "query": {
            "ids": {
                "values": [output_dataset_id],
            }
        },
        "fields": []
    }
    #index = "grq_{}_s1-gunw-ifg-cfg".format(version)
    result = query_es(query, index)
    return False if len(result) == 0 else True

def get_request_id_from_machine_tag(machine_tag):
    if isinstance(machine_tag, str):
        machine_tag = machine_tag.strip().split(',')
    return util.get_request_id(machine_tag)

def create_output_metadata(request_submitted_md, request_data):
    exclude_List = ['id', 'metadata',  'images', 'prov', 'geojson_polygon']
    
    request_data = request_data["_source"]
    for k in request_data:
        logger.info(k)
        if k not in exclude_List:
            request_submitted_md[k] = request_data[k]

    logger.info("request_submitted_md : {}".format(json.dumps(request_submitted_md, indent=2)))
    return request_submitted_md    
    
def main():
    """Main."""

    # read in context
    context_file = os.path.abspath("_context.json")
    if not os.path.exists(context_file):
        raise RuntimeError
    with open(context_file) as f:
        ctx = json.load(f)

    machine_tag = ctx["machine_tag"]
    acqlist_version = ctx["runconfig-acqlist_version"]
    output_dataset_version = ctx['output_dataset_version']
    
    # build args
    project = util.get_value(ctx, "project", "aria")
    if type(project) is list:
        project = project[0]

    job_type, job_version = ctx['job_specification']['id'].split(':')

    request_id = get_request_id_from_machine_tag(machine_tag)
    if not request_id:
        raise Exception("request id not found in machine tag : {}".format(machine_tag))

    es_index = "grq_{}_runconfig-acq-list".format(acqlist_version)
    #output_dataset_index = "grq_{}_s1-gunw-ifg-cfg".format(output_dataset_version)
    output_dataset_index = "grq"
    #request_data = query_es("grq", request_id)
    request_data = util.get_complete_grq_data(request_id)[0]
    logger.info(json.dumps(request_data, indent = 4))

    request_submitted_md = {}
    request_submitted_md = create_output_metadata(request_submitted_md, request_data)

    #request_submitted_md[""] = request_data.get("", None)
    request_submitted_md["program_pi_id"] = request_data.get("program_pi_id", None)
    request_submitted_md[""] = request_data.get("", None)
    request_submitted_md[""] = request_data.get("", None)
    request_submitted_md[""] = request_data.get("", None)
    request_submitted_md[""] = request_data.get("", None)

    program_pi_id = request_data



    acqlists = get_acqlists_by_request_id(request_id, acqlist_version)
    logger.info("Found {} matching acq-list datasets".format(len(acqlists)))
    for acqlist in acqlists:
        logger.info(json.dumps(acqlist, indent=2))
        process_acqlist_localization(acqlist['metadata'], job_type, job_version, project)
        tag_list = acqlist['metadata'].get("tags", [])
        acq_info = {}
        for acq in acqlist['metadata']['master_acquisitions']:
            acq_info[acq] = get_acq_object(acq, "master")
        for acq in acqlist['metadata']['slave_acquisitions']:
            acq_info[acq] = get_acq_object(acq, "slave")
        if all_slcs_exist(list(acq_info.keys()), acq_version, slc_version):
            if output_dataset_type == "ifgcfg":
                prod_dir = publish_ifgcfg_data(acq_info, acqlist['metadata']['project'], acqlist['metadata']['job_priority'],
                                    acqlist['metadata']['dem_type'], acqlist['metadata']['track_number'], acqlist['metadata']['tags'],
                                    acqlist['metadata']['starttime'], acqlist['metadata']['endtime'],
                                    acqlist['metadata']['master_scenes'], acqlist['metadata']['slave_scenes'],
                                    acqlist['metadata']['master_acquisitions'], acqlist['metadata']['slave_acquisitions'],
                                    acqlist['metadata']['orbitNumber'], acqlist['metadata']['direction'],
                                    acqlist['metadata']['platform'], acqlist['metadata']['union_geojson'],
                                    acqlist['metadata']['bbox'], acqlist['metadata']['full_id_hash'],
                                    acqlist['metadata']['master_orbit_file'], acqlist['metadata']['slave_orbit_file'], tag_list)
                logger.info(
                    "Created ifg-cfg {} for acq-list {}.".format(prod_dir, acqlist['id']))
            elif output_dataset_type == "runconfig-topsapp":
                prod_dir = publish_topsapp_runconfig_data(acq_info, acqlist['metadata']['project'], acqlist['metadata']['job_priority'],
                                    acqlist['metadata']['dem_type'], acqlist['metadata']['track_number'], acqlist['metadata']['tags'],
                                    acqlist['metadata']['starttime'], acqlist['metadata']['endtime'],
                                    acqlist['metadata']['master_scenes'], acqlist['metadata']['slave_scenes'],
                                    acqlist['metadata']['master_acquisitions'], acqlist['metadata']['slave_acquisitions'],
                                    acqlist['metadata']['orbitNumber'], acqlist['metadata']['direction'],
                                    acqlist['metadata']['platform'], acqlist['metadata']['union_geojson'],
                                    acqlist['metadata']['bbox'], acqlist['metadata']['full_id_hash'],
                                    acqlist['metadata']['master_orbit_file'], acqlist['metadata']['slave_orbit_file'], tag_list)
                logger.info(
                    "Created runconfig-topsapp {} for runconfig-acqlist {}.".format(prod_dir, acqlist['id']))

            if output_dataset_exists(prod_dir, output_dataset_version, output_dataset_index):
                logger.info(
                    "Not ingesting {} {}. Already exists.".format(output_dataset_type, prod_dir))
            else:
                ingest(prod_dir, 'datasets.json', app.conf.GRQ_UPDATE_URL,
                       app.conf.DATASET_PROCESSED_QUEUE, os.path.abspath(prod_dir), None)
                logger.info("Ingesting {} {}.".format(output_dataset_type, prod_dir))
            shutil.rmtree(prod_dir)
        else:
            logger.info(
                "Not creating {} for acq-list {}.".format(output_dataset, acqlist['id']))


if __name__ == "__main__":
    try:
        status = main()
    except (Exception, SystemExit) as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
    sys.exit(status)
