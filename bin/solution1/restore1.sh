#!/bin/bash


##### Program arguments ###########
root_backup_dir="file:///tmp/backup/hbase-backups"
backup_id=
restore_tables=

options="-Dmapreduce.framework.name=local"

restoreTables() {
   logInfo "Restoring backup $backup_id for table: $restore_tables"
   hbase backup2 $options restore \
       -backup_root_path $root_backup_dir \
       -backup_id $backup_id \
       -tables $restore_tables

}
