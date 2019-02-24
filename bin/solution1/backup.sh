#!/bin/bash

. ./backup-utils.sh

########## Hbase backup based on snapshots #############

##### Program arguments ###########

root_backup_dir="file:///tmp/backup/hbase-backups"
tables_list_file="/etc/backup/hbase-tables-list.txt"
rollup="7"
mappers=7
yarn_queue="default"

#options="-Dmapreduce.job.queuename=$yarn_queue"
options="-Dmapreduce.framework.name=local"

########## Backup ###########################################

logInfo "Running the script using parameters: \n\
         root_backup_dir: $root_backup_dir, \n\
         tables_list_file: $tables_list_file, \n\
         rollup: $rollup,  \n\
         mappers: $mappers,  \n\
         yarn_queue: $yarn_queue \n\
         options: $options "

### Read hbase tables lists and build a backup set

IFS=$'\n'

# Convert the list file tables into an array
hbase_tables_array=$(<"$tables_list_file")


createBackup() {

   hbase_tables=$(join , ${hbase_tables_array[@]})

   logInfo "Begin backup of tables: $hbase_tables"

   if [ $rollup -gt 0 ]
   then
     rollup_args=" -rollup $rollup"
     logInfo "Creating snapshot backup for tables: $hbase_tables in the backup directory: $root_backup_dir, roll up: $rollup_args "
     hbase backup1 $options create -tables "$hbase_tables" -backup_root_path $root_backup_dir -rollup $rollup -mappers $mappers
   else
     logInfo "Creating snapshot backup for tables: $hbase_tables in the backup directory: $root_backup_dir, no roll up"
     hbase backup1 $options create -tables "$hbase_tables" -backup_root_path $root_backup_dir -mappers $mappers
   fi

   exitOnError $? "Faild to create backup"

}

createBackup

