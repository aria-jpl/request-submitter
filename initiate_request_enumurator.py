#!/usr/bin/env python3

from builtins import str
import os, sys, time, json, requests, logging, traceback
import orbit_acquisition_selector
import request_enumurator

def main():

    
    context_file = os.path.abspath("_context.json")
    if not os.path.exists(context_file):
        raise RuntimeError("Context file doesn't exist.")

    orbit_acq_selections = orbit_acquisition_selector.resolve_acqs(context_file)
    request_enumurator.enumerate_acquisations(orbit_acq_selections)


if __name__ == '__main__':
    
    try: status = main()
    except (Exception, SystemExit) as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
    sys.exit(status)
    
