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
export STANDARD_PRODUCT_HOME=$HOME/request
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
echo -n "Running initiate_request_localizer: " 1>&2
date 1>&2
echo python --version
python $BASE_PATH/initiate_request_localizer.py > initiate_request_localizer.log 2>&1
STATUS=$?
echo -n "Finished running initiate_request_localizer: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to run initiate_request_localizer." 1>&2
  cat initiate_request_localizer.log 1>&2
  echo "{}"
  exit $STATUS
fi
