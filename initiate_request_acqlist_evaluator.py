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

from hysds.celery import app
from hysds.dataset_ingest import ingest

from standard_product_localizer import publish_topsapp_runconfig_data, publish_ifgcfg_data, get_acq_object


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


def get_acqlists_by_acqid(acq_id, acqlist_version):
    """Return all acq-list datasets that contain the acquisition ID."""

    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"system_version.raw": acqlist_version}},
                    {
                        "bool": {
                            "should": [
                                {
                                    "term": {
                                        "metadata.master_acquisitions.raw": acq_id
                                    }
                                },
                                {
                                    "term": {
                                        "metadata.slave_acquisitions.raw": acq_id
                                    }
                                }
                            ]
                        }
                    },
                ]
            }
        },
        "partial_fields": {
            "partial": {
                "exclude": ["city", "context", "continent"],
            }
        }
    }
    es_index = "grq_{}_s1-gunw-acq-list".format(acqlist_version)
    result = query_es(query, es_index)

    if len(result) == 0:
        logger.info("Couldn't find acq-list containing acquisition ID: {}".format(acq_id))
        sys.exit(0)

    return [i['fields']['partial'][0] for i in result]

def get_acqlists_by_acqid_index(acq_id, acqlist_version, es_index):
    """Return all acq-list datasets that contain the acquisition ID."""

    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"system_version.raw": acqlist_version}},
                    {
                        "bool": {
                            "should": [
                                {
                                    "term": {
                                        "metadata.master_acquisitions.raw": acq_id
                                    }
                                },
                                {
                                    "term": {
                                        "metadata.slave_acquisitions.raw": acq_id
                                    }
                                }
                            ]
                        }
                    },
                ]
            }
        },
        "partial_fields": {
            "partial": {
                "exclude": ["city", "context", "continent"],
            }
        }
    }
    #es_index = "grq_{}_s1-gunw-acq-list".format(acqlist_version)
    result = query_es(query, es_index)

    if len(result) == 0:
        logger.info("Couldn't find acq-list containing acquisition ID: {}".format(acq_id))
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


def main():
    """Main."""

    # read in context
    context_file = os.path.abspath("_context.json")
    if not os.path.exists(context_file):
        raise RuntimeError
    with open(context_file) as f:
        ctx = json.load(f)

    # resolve acquisition id from slc id
    slc_id = ctx['slc_id']
    slc_version = ctx['slc_version']
    acq_version = ctx['acquisition_version']
    output_dataset_type = ctx.get("output_dataset_type", "ifgcfg")
    acq_id = resolve_acq(slc_id, acq_version)
    logger.info("acq_id: {}".format(acq_id))

    # pull all acq-list datasets with acquisition id in either master or slave list
    output_dataset_version = ctx.get('output_dataset_version', None)
    if not output_dataset_version:
        if 'ifgcfg_version' in ctx:
            output_dataset_version = ctx['ifgcfg_version']
        else:
            err_msg = "output_dataset_version NOT found in context"
            logger.info(err_msg)
            raise Exception(err_msg)

    acqlist_version = ctx['acqlist_version']
    es_index = "grq_{}_s1-gunw-acq-list".format(acqlist_version)
    output_dataset_index = "grq_{}_s1-gunw-ifg-cfg".format(output_dataset_version)
    if output_dataset_type == "runconfig-topsapp":
        es_index = "grq_{}_s1-gunw-runconfig-acq-list".format(acqlist_version)
        output_dataset_index = "grq"

    acqlists = get_acqlists_by_acqid_index(acq_id, acqlist_version, es_index)
    logger.info("Found {} matching acq-list datasets".format(len(acqlists)))
    for acqlist in acqlists:
        logger.info(json.dumps(acqlist, indent=2))
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
