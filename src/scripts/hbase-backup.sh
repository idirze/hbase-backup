#!/bin/bash

############# Help & Usage  ################################

function usage(){
        printf "Usage :\n"
	printf " --root_backup_dir        : Root directory where to store the backups, ex.: hdfs:///namenode:8080/path/to/backup/dir or file:///path/to/backup/dir\n"
	printf " --set                    : Backup set name\n"
	printf " --rollup                 : Number of the incremental backups before running the merge\n"
	printf " -h                       : Help\n"
        exit 0
}

if [ $# -eq 0 ]
then
	usage
fi

############ Parse arguments #############################

root_backup_dir=$1
set=$2
rollup=$3

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


logInfo "Running the script using parameters: --root_backup_dir: $root_backup_dir, --set: $set, --rollup: $rollup"

IFS=$'\n'

fullBackup=($(hbase backup history  | grep FULL | grep COMPLETE | awk -F{ '{print $2}'  |awk -F, '{print $1}' | awk -F= '{print $2}'))
incBackups=($(hbase backup history  | grep INCREMENTAL | awk -F{ '{print $2}'  |awk -F, '{print $1}' | awk -F= '{print $2}'))

hasFullBackup(){

  if [ ${#fullBackup[@]} -eq 0 ]
  then
    return 1
  else
    return 0
  fi
}

createFullBackup() {
   hbase backup -Dmapreduce.framework.name=local \
      create full "$root_backup_dir"  \
      -s "$set"

   if [ $? -ne 0 ]
   then
     logError "Full backup failed"
     exit 1
   fi
}

createInBackup() {
  hbase backup -Dmapreduce.framework.name=local \
        create incremental "$root_backup_dir"  \
        -s "$set"
   if [ $? -ne 0 ]
   then
     logError "Incremental backup failed"
     exit 1
   fi
}

repair(){
  logInfo "Repair backup"
  hbase backup repair
}

mergeIncBackups() {

  if [ "${#incBackups[@]}" -gt "$rollup" ]
  then
     toMerge=$(join , ${incBackups[@]})
     logInfo "Merging backups: $toMerge"
     sleep 3
     hbase backup -Dmapreduce.framework.name=local merge  "$toMerge"
     if [ $? -ne 0 ]
     then
       logError "Merge backups $toMerge failed"
       exit 1
     fi
  fi
}

if ! hasFullBackup
then
  logInfo "Trigger initial full backup"
  createFullBackup
else
  logInfo "Trigger an incremental backup"

  logInfo "STEP 1 - Merge last incremental backups"
  mergeIncBackups

  logInfo "STEP2 - Create an incremental backup"
  createInBackup
fi

logInfo "Backup complete"

