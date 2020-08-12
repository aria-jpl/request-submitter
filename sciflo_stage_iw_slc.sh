#!/bin/bash
set -e

BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# source PGE env
export PYTHONPATH=$BASE_PATH:$PYTHONPATH
export PATH=$BASE_PATH:$PATH
export PYTHONPATH=${PYTHONPATH}:${HOME}/verdi/etc
# source ISCE env
export GMT_HOME=/usr/local/gmt
export ARIAMH_HOME=$HOME/ariamh
export STANDARD_PRODUCT_HOME=$HOME/standard_product
source $ARIAMH_HOME/isce.sh
source $ARIAMH_HOME/giant.sh
export TROPMAP_HOME=$HOME/tropmap
export UTILS_HOME=$ARIAMH_HOME/utils
export GIANT_HOME=/usr/local/giant/GIAnT
export MAL_HOME=${HOME}/verdi/ops/multi_acquisition_localizer
export PYTHONPATH=${HOME}/verdi/etc:$ISCE_HOME/applications:$ISCE_HOME/components:$BASE_PATH:$ARIAMH_HOME:$TROPMAP_HOME:$GIANT_HOME:$MAL_HOME:$PYTHONPATH
export PATH=$BASE_PATH:$TROPMAP_HOME:$GMT_HOME/bin:$PATH

# source environment
source $HOME/verdi/bin/activate


echo "##########################################" 1>&2
echo -n "Running S1 create interferogram sciflo: " 1>&2
date 1>&2
/usr/bin/python $BASE_PATH/sciflo_stage_iw_slc.py > sciflo_stage_iw_slc.log 2>&1
STATUS=$?
echo -n "Finished running stage iw slc sciflo: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to run stage iw slc sciflo." 1>&2
  cat sciflo_stage_iw_slc.log 1>&2
  echo "{}"
  exit $STATUS
fi
