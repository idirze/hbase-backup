#!/bin/bash

. ./backup-utils.sh

########## Hbase backup for incremental backups #############

##### Program arguments ###########

root_backup_dir="file:///tmp/backup/hbase-backups"
tables_list_file="/etc/backup/hbase-tables-list.txt"
rollup="7"
yarn_queue="default"
workers="3"
options="-Dmapreduce.framework.name=local -Dcreate.initial.snapshot.on.restore=yes -Dcreate.initial.snapshot.on.backup=yes"

BACKUP_SET_NAME="incremental_backupset"

########## Backup ###########################################

logInfo "Running the script using parameters: \n\
         root_backup_dir: $root_backup_dir, \n\
         tables_list_file: $tables_list_file, \n\
         rollup: $rollup  \n\
         workers: $mappers,  \n\
         yarn_queue: $yarn_queue \n\
         options: $options "

### Read hbase tables lists and build a backup set

IFS=$'\n'

# Convert the list file tables into an array
hbase_tables_array=$(<"$tables_list_file")


##### Update the backup set with the most recent provided file ###
##################################################################
function createOrUpdateBackupSet() {

  bk_set_name=$1
  hbase_tables_list_file=$2

  backup_set_tables=$(join , ${hbase_tables_array[@]})
  logInfo "Creating backup set $bk_set_name with tables: $backup_set_tables"
  hbase backup2 set delete $bk_set_name
  hbase backup2 set add $bk_set_name "$backup_set_tables"

  exitOnError $? "Failed to create backup set, please ensure the backup was enabled and/or your tables exist on hbase"

}

##### Create full or incremental backup depending on the tables are already full backuped or not ###
####################################################################################################
createBackup() {

   fullBackup=($(hbase backup2 history  | grep FULL | grep COMPLETE | awk -FTables={ '{print $2}' |awk -F} '{print $1}' | tr , \n ))

   # Get the list of tables which requires full back
   need_full_backup=()
   need_inc_backup=()

   for table in $hbase_tables_array
   do
       if ! containsElement "$table" "${fullBackup[@]}"
       then
           logInfo "The table $table needs to go throught full backup"
           need_full_backup+=($table)
       else
           logInfo "The table $table needs to go throught incremental backup"
           need_inc_backup+=($table)
       fi
   done

   if [ ${#need_full_backup[@]} -gt 0 ]
   then
     logInfo "Creating full backup for tables:  ${need_full_backup[@]}"
     hbase backup2 $options create full "$root_backup_dir"  -t $(join , "${need_full_backup[@]}") -w $workers -q $yarn_queue
     repairAndExitOnError $? "Failed to create full backup for tables ${need_full_backup[@]}"
   fi

   if [ ${#need_inc_backup[@]} -gt 0 ]
   then
     logInfo "Creating incremental backup for tables:  ${need_inc_backup[@]}"
     hbase backup2 $options create incremental "$root_backup_dir"  -t $(join , "${need_inc_backup[@]}") -w $workers -q $yarn_queue
     repairAndExitOnError $? "Failed to create incremental backup for ${need_inc_backup[@]}"
   fi

   # For restoring the whole tables
   createOrUpdateBackupSet $BACKUP_SET_NAME $tables_list_file

}

###### Merge every rollup number of incremental backups ###
###########################################################
mergeIncBackups() {

  incBackups=($(hbase backup2 history  | grep INCREMENTAL | awk -F{ '{print $2}'  |awk -F, '{print $1}' | awk -F= '{print $2}'))
  if [ "${#incBackups[@]}" -gt "$rollup" ]
  then
     toMerge=$(join , ${incBackups[@]})
     logInfo "Merging backups: $toMerge"
     hbase backup2 $options merge  "$toMerge"

     repairAndExitOnError $? "Failed to merge incremental backups: $toMerge"

  fi
}


createBackup
mergeIncBackups

ret_code=$?
logInfo "Backup complete, status code: $ret_code"

exit $ret_code

