#!/bin/bash

SERVICE=$(basename "$0" |awk -F- '{ print $1 }' | tr '[:lower:]' '[:upper:]')
logfileName=$(basename "$0" |awk -F- '{ print $1 }' | tr '[:upper:]' '[:lower:]')
logfileName="${logfileName}-backup.out"

mkdir -p  /var/log/backup
logfile=/var/log/backup/$logfileName

########## Logging #########################################
logInfo() {
  log "$(date) - INFO - $SERVICE - $1"
}

logWarn() {
 log "$(date) - WARN - $SERVICE - $1"
}

logError() {
  log "$(date) - ERROR - $SERVICE - $1"
}

log() {

  msg="$1"
  echo -e "$msg"
  echo -e "$msg" >> $logfile

}


########### Common Utils ###########################################
# join elements of an array
function join { local IFS="$1"; shift; echo "$*"; }

containsElement () {
  local e match="$1"
  shift
  for e; do [[ "$e" == "$match" ]] && return 0; done
  return 1
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

########### Hbase Utils ###########################################

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
