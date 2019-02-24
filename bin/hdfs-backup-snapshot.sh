#!/bin/bash

. ./backup-utils.sh

########## hdfs backup based on snapshots #############

##### Program arguments ###########

source_dirs_file_list="/etc/backup/hdfs-source-dirs-list"
dest_backup_dir="/tmp/hdfs-backup"
rollup="1"
mappers=3
yarn_queue="default"

#options="-Dmapreduce.job.queuename=$yarn_queue"
options="-Dmapreduce.framework.name=local"

hdfs_sudo_user="hdfs"
backup_sudo_user="hdfs"

# Set the backup name to the current date
backup_name_prefix="its-backup"
backup_name="${backup_name_prefix}-$(date '+%Y%m%d%H%M')"

### bacckup ###########################################

logInfo "Running the script using parameters: \n\
         source_dirs_file_list: $source_dirs_file_list, \n\
         dest_backup_dir: $dest_backup_dir, \n\
         rollup: $rollup  \n\
         mappers: $mappers, \n\
         yarn_queue: $yarn_queue \n\
         options: $options  \n\
         hdfs_sudo_user: $hdfs_sudo_user \n\
         backup_name: $backup_name"


### Read source dirs list

IFS=$'\n'

# Convert the list file directories into an array
source_dirs_array=$(<"$source_dirs_file_list")


allowSnapshot() {

   dirs="$1"
   for source_dir in $dirs
   do
        logInfo "Allow snapshot on directory $source_dir"
        sudo -u $hdfs_sudo_user hdfs dfsadmin -allowSnapshot $source_dir
   done

}

createSnapshot(){
    dir="$1"
    bk_name="$2"
    logInfo "Create snapshot for directory $dir -> $dir/.snapshot/$bk_name"
    sudo -u $backup_sudo_user hdfs dfs -createSnapshot "$dir" "$bk_name"
    if [ $? -ne 0 ]
    then
      logError "Create snapshot failed, please check if the user $backup_sudo_user has read/write permission on the folder: $dir"
      return 1
    fi
    return 0
}

copySnaphsot(){
    src="$1"
    dest="$2"
    logInfo "Copy the snapshot into the target directory; src: $src, dest: $dest"
    sudo -u $backup_sudo_user hadoop distcp -skipcrccheck -update $src $dest
    if [ $? -ne 0 ]
    then
      logError "Copy snapshot failed, please check the backup logs: "
      return 1
    fi
    return 0
}

deleteSnapshot() {
  src="$1"
  bk_name="$2"
  logInfo "Delete the snapshot $bk_name from the source directory; src: $src"
  sudo -u $backup_sudo_user hdfs dfs -deleteSnapshot "$src" "$bk_name"
  if [ $? -ne 0 ]
  then
    logError "Delete snapshot failed, please check the backup logs: "
    return 1
  fi
}

backupDir() {

  src_dir="$1"
  bkName="$2"
  if copySnaphsot "$src_dir/.snapshot/$bkName" "$dest_backup_dir/$src_dir"
  then
     # Snapshot the destination directory
     if createSnapshot "$dest_backup_dir/$src_dir" "$bkName"
     then
       # Delete the snapshot of the source directory
       echo "we delete this: $src_dir $bkName"
       deleteSnapshot "$src_dir" "$bkName"
     fi
  fi
}

backupDirs() {

  dirs="$1"
  bkName="$2"
  for source_dir in $dirs
  do
     if createSnapshot "$source_dir" "$bkName"
     then
       backupDir "$source_dir" "$bkName"
     fi
  done

}

replayFailedBackups() {

  dirs="$1"
  for source_dir in $dirs
  do
     existing_source_snapshots=($(sudo -u $backup_sudo_user hdfs dfs -ls $source_dir/.snapshot |grep $backup_name_prefix | awk '{ print $8 }' | awk -F/ '{ print  $NF }'))

     if [ ${#existing_source_snapshots[@]} -gt 0 ]
     then
       logInfo "Found existing source snapshots for source dir $source_dir: ${existing_source_snapshots[@]}"

       for srcSnapshot in $existing_source_snapshots
       do
         logInfo "The source snapshot $source_dir/.snapshot/$srcSnapshot was not yet backuped, replaying ..."
         backupDir "$source_dir" "$srcSnapshot"
       done
    fi
  done

}

rollupBackups() {

  keep=$1

  existing_backups=($(sudo -u $backup_sudo_user hdfs dfs -ls $dest_backup_dir | grep $backup_name_prefix | awk '{ print $8 }' | awk -F/ '{ print  $NF }' | sort | uniq))
  size=${#existing_backups[@]}
  logInfo "Rollup: found $size backups, weed need to keep $keep backups"
  if [ $size -gt $keep ]
  then
    to_be_deleted=${existing_backups[@]:$rollup:$size}
    logInfo "Rollup: deleting backups: ${to_be_deleted[@]}"
  fi

}

rollupBackups $rollup

# Allow snapshots on both source and dest directories
allowSnapshot "${source_dirs_array[@]}"
for srcDir in $source_dirs_array
do
  sudo -u  $backup_sudo_user hdfs dfs -mkdir -p "$dest_backup_dir/$srcDir"
  allowSnapshot "$dest_backup_dir/$srcDir"
done

backupDirs "${source_dirs_array[@]}"  "$backup_name"
replayFailedBackups "${source_dirs_array[@]}"


