#!/bin/bash

IFS=$'\n'

############# Help & Usage  ################################

function usage(){
        printf "\nIncremental Backup and Restore solution - Patchs based hbase2 backport to hbase 1.2.1.\n"
        printf "See more: https://docs.hortonworks.com/HDPDocuments/HDP3/HDP-3.1.0/hbase-data-access/content/hbase_bar.html\n"
        printf "\nThis script wraps the regular commands\n"
        printf "\nUsage :\n\n"
        printf " ******** Create Backups ******** :\n"
	printf "  --root_backup_dir        : Root directory where to store the backups, ex.: hdfs:///namenode:8080/path/to/backup/dir or file:///path/to/backup/dir\n"
	printf "  --tables_list_file       : File containing the list of the tables. Ex.: \n"
        printf "                           \thbaseTables.txt:\n"
        printf "                           \tns1:t1\n"
        printf "                           \tns1:t2\n"
        printf "                          \tns3:t1\n"
	printf "  --rollup                 : Number of the incremental backups before running the merge\n\n"
        printf " ******** Restore backups ******** :\n"
        printf "  --restore_tables                 : Hbase tables list to restore\n\n"
        printf "  --backup_id                      : Backup id to restore, use show_history to get image ids"
        printf " ******** Show history ******** :\n"
        printf "  --show_history           : Show backups history\n"
	printf "  --help                   : Show usage\n"
        exit 0
}

if [ $# -eq 0 ]
then
	usage
fi

############ Parse arguments #############################

ARGUMENT_LIST=(
    "root_backup_dir"
    "tables_list_file"
    "rollup"
    "restore_tables"
    "backup_id"
    "show_history"
    "help"
)


# read arguments
opts=$(getopt \
    --longoptions "$(printf "%s:," "${ARGUMENT_LIST[@]}")" \
    --name "$(basename "$0")" \
    --options "" \
    -- "$@"
)

eval set --$opts

# Set defaults
show_history=0 # False

while [[ $# -gt 0 ]]; do
    case "$1" in
        --root_backup_dir)
            root_backup_dir=$2
            shift 2
            ;;

        --tables_list_file)
            tables_list_file=$2
            shift 2
            ;;

        --rollup)
            rollup=$2
            shift 2
            ;;
        --restore_tables)
            restore_tables=$2
            shift 2
            ;;
        --backup_id)
            backup_id=$2
            shift 2
            ;;
        --show_history)
            show_history=1
            shift 2
            ;;
        --help)
            shift 2
            usage
            ;;
        *)
            break
            ;;
    esac
done

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

### Read hbase tables lists
show_history() {
  if [ $show_history -eq 1 ]
  then
     hbase backup history  | grep -E "FULL|INCREMENTAL"
     exit 0
  fi
}

show_history

logInfo "Running the script using parameters: --root_backup_dir: $root_backup_dir, --tables_list_file: $tables_list_file, --rollup: $rollup"

### Read hbase tables lists and build a backup set

BACKUP_SET_NAME="incremental_backup"

function createBackupSetIfNotExists() {
   backup_set_name=$(hbase backup set list |grep $BACKUP_SET_NAME | tail -1 |awk -F={  '{ print $1 }')
   if [ -z "$backup_set_name" ]
   then
      hbase_tables_array=$(<"$tables_list_file")
      #backup_set_tables=$(hbase backup set list |tail -1 |awk -F={  '{ print $2 }' |awk -F}  '{ print $1 }')
      #read -ra backup_set_tables_array <<< "$backup_set_tables"
      backup_set_tables=$(join , ${hbase_tables_array[@]})
      logInfo "Creating backup set $BACKUP_SET_NAME with tables: $backup_set_tables"
      hbase backup set add $BACKUP_SET_NAME "$backup_set_tables"
      if [ $? -ne 0 ]
      then
        logError "Failed to create backup set"
        exit 1
      fi
   fi
}

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
      -s "$BACKUP_SET_NAME"

   if [ $? -ne 0 ]
   then
     logError "Full backup failed"
     exit 1
   fi
}

createInBackup() {
  hbase backup -Dmapreduce.framework.name=local \
        create incremental "$root_backup_dir"  \
        -s "$BACKUP_SET_NAME"
   if [ $? -ne 0 ]
   then
     logError "Incremental backup failed"
     exit 1
   fi
}

restoreBackup(){
   hbase restore -Dmapreduce.framework.name=local -t $restore_tables \
   -o $root_backup_dir $backup_id

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


createBackupSetIfNotExists

logInfo "---Restoring backup: $backup_id for tables: $restore_tables"
if [ ! -z "$restore_tables" ]
then
  logInfo "Restoring backup: $backup_id for tables: $restore_tables"
  restoreBackup
elif ! hasFullBackup
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