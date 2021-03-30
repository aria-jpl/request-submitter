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

from request_localizer import query_es, get_acq_object, process_acqlist_localization


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


def get_acqlists_by_request_id(request_id, acqlist_version):
    """Return all acq-list datasets that contain the acquisition ID."""

    query = {
        "query": {
            "bool": {
                "must": [
                  {
                    "term": {
                      "dataset.raw": "runconfig-acq-list"
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
    es_index = "grq_{}_runconfig-acq-list".format(acqlist_version)
    result = query_es(query, es_index)

    if len(result) == 0:
        logger.info("Couldn't find acq-list containing Request ID: {}".format(request_id))
        sys.exit(0)

    return [i['fields']['partial'][0] for i in result]


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
    output_dataset_version = ctx.get('output_dataset_version', "v2.0.0")
    geocoded_unfiltered_coherence = ctx['geocoded_unfiltered_coherence']
    geocoded_unfiltered_wrapped_phase = ctx['geocoded_unfiltered_wrapped_phase']
    group_id = ctx.get("group_id", "")

    output_dataset_type = "request-submit"
    output_dataset_index = "grq_{}_request-submit".format(output_dataset_version)
    
    esa_download_queue = util.get_value(ctx, "esa_download_queue", "slc-sling-extract-scihub")
    asf_ngap_download_queue = util.get_value(ctx, "asf_ngap_download_queue", "slc-sling-extract-asf")
    spyddder_sling_extract_version = util.get_value(ctx, "spyddder_sling_extract_version", "ARIA-446_singularity")
    multi_acquisition_localizer_version = util.get_value(ctx, "multi_acquisition_localizer_version", "ARIA-446")
    destination_type = "local"

    # build args
    project = util.get_value(ctx, "project", "aria")
    if type(project) is list:
        project = project[0]

    job_type, job_version = ctx['job_specification']['id'].split(':')

    request_id = get_request_id_from_machine_tag(machine_tag)
    if not request_id:
        raise Exception("request id not found in machine tag : {}".format(machine_tag))


    #request_data = query_es("grq", request_id)
    request_data = util.get_complete_grq_data(request_id)[0]
    logger.info(json.dumps(request_data, indent = 4))
  
    request_submitted_id = request_id.replace("request", "request-s1gunw-submitted", 1)
    request_submitted_md = {}
    request_submitted_md = create_output_metadata(request_submitted_md, request_data)
    if "master_scenes" in request_submitted_md:
        request_submitted_md["master_scenes"] = util.add_local_list(request_submitted_md["master_scenes"])
    if "slave_scenes" in request_submitted_md:
        request_submitted_md["slave_scenes"] = util.add_local_list(request_submitted_md["slave_scenes"])

    request_submitted_md['geocoded_unfiltered_coherence'] = geocoded_unfiltered_coherence
    request_submitted_md['geocoded_unfiltered_wrapped_phase'] = geocoded_unfiltered_wrapped_phase

    acqlists = get_acqlists_by_request_id(request_id, acqlist_version)

    logger.info("Found {} matching acq-list datasets".format(len(acqlists)))
    request_submitted_md["tags"] = []

    for acqlist in acqlists:
        input_metadata = acqlist["metadata"]
        logger.info("input_metadata : \n{}".format(json.dumps(input_metadata, indent=2)))

        tag_list = acqlist['metadata'].get("tags", [])
        program_pi_id = request_submitted_md["program_pi_id"]
        tag_list.append(program_pi_id)
        if group_id:
            tag_list.append(group_id)

        logger.info("tag_list : {} program_pi_id : {} group_id : {}".format(tag_list, program_pi_id, group_id))
        request_submitted_md["tags"].extend(tag_list)

    request_submitted_md["tags"] = list(set(request_submitted_md["tags"])


    if not util.dataset_exists(request_submitted_id, "request-submit"):
        prod_dir = util.publish_dataset(request_submitted_id, request_submitted_md, output_dataset_version)
        logger.info("prod_dir : {}".format(prod_dir))

        if util.dataset_exists(prod_dir, "request-submit"):
            logger.info(
                "Not ingesting {} {}. Already exists.".format(output_dataset_type, prod_dir))
        else:
            ingest(prod_dir, 'datasets.json', app.conf.GRQ_UPDATE_URL,
                app.conf.DATASET_PROCESSED_QUEUE, os.path.abspath(prod_dir), None)
            logger.info("Ingesting {} {}.".format(output_dataset_type, prod_dir))
            shutil.rmtree(prod_dir)

    for acqlist in acqlists:
        input_metadata = acqlist["metadata"]
        logger.info("input_metadata : \n{}".format(json.dumps(input_metadata, indent=2)))
        logger.info("calling process_acqlist_localization for above acqlist")
        process_acqlist_localization(input_metadata, esa_download_queue, asf_ngap_download_queue, spyddder_sling_extract_version, multi_acquisition_localizer_version, job_type, job_version, project, destination_type, request_id)
        logger.info("returned from process_acqlist_localization for above acqlist")


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
