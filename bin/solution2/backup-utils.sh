#!/bin/bash



########## Logging #########################################
logInfo() {
  echo -e "$(date) - INFO - $1"
}

logError() {
  echo -e "$(date) - ERROR - $1"
}

########### Utils ###########################################
# join elements of an array
function join { local IFS="$1"; shift; echo "$*"; }

containsElement () {
  local e match="$1"
  shift
  for e; do [[ "$e" == "$match" ]] && return 0; done
  return 1
}

repair(){
  logInfo "Repair backup"
  hbase backup2 repair
}

repairAndExitOnError() {

    exit_code=$1
    msg=$2

    if [ $exit_code -ne 0 ]
    then
       logError "$2"
       repair
       exit 1
    fi

}

exitOnError() {
    exit_code=$1
    msg=$2

    if [ $exit_code -ne 0 ]
    then
       logError "$2"
       exit 1
    fi
}