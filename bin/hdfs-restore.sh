#!/bin/bash

. ./backup-utils.sh

##### Program default arguments ###########

mappers=3
yarn_queue="default"
############# Help & Usage  ################################

function usage(){
    printf "\nUsage: hdfs restore options\n\n"
    printf "where options: \n"
	printf "  --root_backup_dir *      : Root backup directory (source), ex.: hdfs:///namenode:8080/path/to/backup/sourceBackupDir or file:///path/to/backup/dir\n"
    printf "  --root_restore_dir *     : Root restore directory (destination): the folder where to restore the backup, ex.: hdfs:///namenode:8080/path/to/backup/targetRestoreDir or file:///path/to/backup/dir\n"
    printf "  --backup_id  *           : The backup_id to restore. \n"
	printf "  --restore_directories    : The list of the directories to restore. Ex.: \n"
    printf "                           \t/path/to/dir1,/path/to/dir2,/path/to/dir3\n"
    printf " --delete_target           : Delete the files existing in the dst but not in src               \n"
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
    "root_restore_dir"
    "backup_id"
    "restore_directories"
    "delete_target"
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
delete_target=no
while [[ $# -gt 0 ]]; do
    case "$1" in
        --root_backup_dir)
            root_backup_dir=$2
            shift 2
            ;;
        --root_restore_dir)
            root_restore_dir=$2
            shift 2
            ;;
        --backup_id)
            backup_id=$2
            shift 2
            ;;

        --restore_directories)
            restore_directories=$2
            shift 2
            ;;
        --delete_target)
            delete_target=$2
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

if [ -z $root_backup_dir ] || [ -z $backup_id ] || [ -z $root_restore_dir ]
then
        logError "The options --root_backup_dir, --backup_id and --root_restore_dir are mandatory"
        usage
fi

logInfo "Running the script using parameters: \n\
         root_backup_dir: $root_backup_dir, \n\
         backup_id: $backup_id, \n\
         restore_directories: $restore_directories  \n\
         yarn_queue: $yarn_queue \n\
         mappers: $mappers \n\
         delete_target: $delete_target "


restoreSnaphsot(){
    src="$1"
    dest="$2"
    logInfo "Copy the snapshot into the target directory; src: $src, dest: $dest"
    if [  "$delete_target" == "yes" ]
    then
      logInfo "Will delete the target"
      hadoop distcp  -p -update -delete -strategy dynamic -m $mappers "$src" "$dest"
    else
      hadoop distcp -p -update  -strategy dynamic -m $mappers "$src" "$dest"
    fi

    if [ $? -ne 0 ]
    then
      logError "Restore snapshot failed, please check the backup logs: "
      exit 1
    fi
    exit 0
}

hdfs dfs -test -d "$root_backup_dir/$backup_id"
if [ $? -ne 0 ]
then
  logError "No backup found for backup_id: $backup_id. No such backup directory: $root_backup_dir/$backup_id"
  exit 1
fi

IFS=","
if [ ! -z "$restore_directories" ]
then
  for d in $restore_directories
  do
    logInfo "Restoring directory: $root_backup_dir/$backup_id/$d into $root_restore_dir"
    restoreSnaphsot "$root_backup_dir/$backup_id/$d" "$root_restore_dir"
    logInfo "Directory $root_backup_dir/$backup_id/$d restored"
  done
  exit 0
fi

restoreSnaphsot "$root_backup_dir/$backup_id" "$root_restore_dir"

