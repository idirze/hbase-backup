#!/bin/bash

IFS=$'\n'

############# Help & Usage  ################################

function usage(){
        printf "\nIncremental Backup and Restore solution - Based on snapshots \n"
        printf "\nUsage :\n\n"
        printf " ******** Create Backups ******** :\n"
	printf "  --root_backup_dir *      : Root directory where to store the backups, ex.: hdfs:///namenode:8080/path/to/backup/dir or file:///path/to/backup/dir\n"
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
rollup=0

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
     hbase backup2 history \
      -backup_root_path $root_backup_dir
     exit 0
  fi
}

show_history

logInfo "Running the script using parameters: --root_backup_dir: $root_backup_dir, --tables_list_file: $tables_list_file, --rollup: $rollup"

### Read hbase tables lists and build a backup set

createBackup() {
    hbase_tables_array=$(<"$tables_list_file")
    hbase_tables=$(join , ${hbase_tables_array[@]})

   logInfo "Begin backup of tables: $hbase_tables"

   if [ $rollup -gt 0 ]
   then
     rollup_args=" -rollup $rollup"
     logInfo "Running backup using arguments: -tables "\"$hbase_tables\"" -backup_root_path $root_backup_dir $rollup_args "
     hbase backup2 -Dmapreduce.framework.name=local create -tables "$hbase_tables" -backup_root_path $root_backup_dir -rollup $rollup
   else
     logInfo "Running backup using arguments: -tables "\"$hbase_tables\"" -backup_root_path $root_backup_dir $rollup_args "
     hbase backup2 -Dmapreduce.framework.name=local create -tables "$hbase_tables" -backup_root_path $root_backup_dir
   fi

   if [ $? -ne 0 ]
   then
     logError "Full backup failed"
     exit 1
   fi
}

restoreTables() {
   logInfo "Restoring backup $backup_id for table: $restore_tables"
   hbase backup2 -Dmapreduce.framework.name=local restore \
       -backup_root_path $root_backup_dir \
       -backup_id $backup_id \
       -tables $restore_tables

}

if [ ! -z "$tables_list_file" ]
then
  createBackup
   if [ $? -ne 0 ]
   then
     logError "Backup failed"
     exit 1
   fi
  logInfo "Backup complete"
elif [ ! -z "$restore_tables" ] && [ ! -z "$backup_id" ]
then
  restoreTables
   if [ $? -ne 0 ]
   then
     logError "Failed to restore data"
     exit 1
   fi
  logInfo "Restore complete"
fi