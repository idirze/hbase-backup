/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.backup.hbase1_2_1.mapreduce;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.hbase1_2_1.BackupInfo;
import org.apache.hadoop.hbase.backup.hbase1_2_1.BackupMergeJob;
import org.apache.hadoop.hbase.backup.hbase1_2_1.HBackupFileSystem;
import org.apache.hadoop.hbase.backup.hbase1_2_1.impl.BackupManifest;
import org.apache.hadoop.hbase.backup.hbase1_2_1.impl.BackupSystemTable;
import org.apache.hadoop.hbase.backup.hbase1_2_1.util.BackupUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.util.Tool;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.apache.hadoop.hbase.backup.hbase1_2_1.util.BackupUtils.succeeded;

/**
 * MapReduce implementation of {@link BackupMergeJob}
 * Must be initialized with configuration of a backup destination cluster
 */
@InterfaceAudience.Private
public class MapReduceBackupMergeJob implements BackupMergeJob {
    public static final Logger LOG = LoggerFactory.getLogger(MapReduceBackupMergeJob.class);

    static final String TABLEINFO_DIR = ".tabledesc";

    protected Tool player;
    protected Configuration conf;

    public MapReduceBackupMergeJob() {
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public void run(String[] backupIds) throws IOException {
        String bulkOutputConfKey;

        // TODO : run player on remote cluster
        player = new MapReduceHFileSplitterJob();
        bulkOutputConfKey = MapReduceHFileSplitterJob.BULK_OUTPUT_CONF_KEY;
        // Player reads all files in arbitrary directory structure and creates
        // a Map task for each file
        String bids = StringUtils.join(backupIds, ",");

        if (LOG.isDebugEnabled()) {
            LOG.debug("Merge backup images " + bids);
        }

        List<Pair<TableName, Path>> processedTableList = new ArrayList<>();
        boolean finishedTables = false;
        Connection conn = ConnectionFactory.createConnection(getConf());
        BackupSystemTable table = new BackupSystemTable(conn);
        //FileSystem fs = FileSystem.get(getConf());

        try {

            // Get exclusive lock on backup system
            table.startBackupExclusiveOperation();
            // Start merge operation
            table.startMergeOperation(backupIds);

            // Select most recent backup id
            String mergedBackupId = BackupUtils.findMostRecentBackupId(backupIds);

            TableName[] tableNames = getTableNamesInBackupImages(backupIds);

            BackupInfo bInfo = table.readBackupInfo(backupIds[0]);
            String backupRoot = bInfo.getBackupRootDir();

            for (int i = 0; i < tableNames.length; i++) {
                LOG.info("Merge backup images for " + tableNames[i]);

                // Find input directories for table
                Path[] dirPaths = findInputDirectories(backupRoot, tableNames[i], backupIds);
                String dirs = StringUtils.join(dirPaths, ",");

                Path bulkOutputPath =
                        BackupUtils.getBulkOutputDir(BackupUtils.getFileNameCompatibleString(tableNames[i]),
                                getConf(), false);
                // Delete content if exists
                FileSystem fsOut = bulkOutputPath.getFileSystem(conf);
                if (fsOut.exists(bulkOutputPath)) {
                    if (!fsOut.delete(bulkOutputPath, true)) {
                        LOG.warn("Can not delete: " + bulkOutputPath);
                    }
                }
                Configuration conf = getConf();
                conf.set(bulkOutputConfKey, bulkOutputPath.toString());
                String[] playerArgs = {dirs, tableNames[i].getNameAsString()};

                player.setConf(getConf());
                int result = player.run(playerArgs);
                if (!succeeded(result)) {
                    throw new IOException("Can not merge backup images for " + dirs
                            + " (check Hadoop/MR and HBase logs). Player return code =" + result);
                }
                // Add to processed table list
                processedTableList.add(new Pair<>(tableNames[i], bulkOutputPath));
                LOG.debug("Merge Job finished:" + result);
            }
            List<TableName> tableList = toTableNameList(processedTableList);
            table.updateProcessedTablesForMerge(tableList);
            finishedTables = true;

            // PHASE 2 (modification of a backup file system)
            // Move existing mergedBackupId data into tmp directory
            // we will need it later in case of a failure
            Path tmpBackupDir = HBackupFileSystem.getBackupTmpDirPathForBackupId(backupRoot,
                    mergedBackupId);
            Path backupDirPath = HBackupFileSystem.getBackupPath(backupRoot, mergedBackupId);

            FileSystem fsIn = backupDirPath.getFileSystem(conf);

            if (!fsIn.rename(backupDirPath, tmpBackupDir)) {
                throw new IOException("Failed to rename " + backupDirPath + " to " + tmpBackupDir);
            } else {
                LOG.debug("Renamed " + backupDirPath + " to " + tmpBackupDir);
            }
            // Move new data into backup dest
            for (Pair<TableName, Path> tn : processedTableList) {
                moveData(backupRoot, tn.getSecond(), tn.getFirst(), mergedBackupId);
            }
            // Update backup manifest
            List<String> backupsToDelete = getBackupIdsToDelete(backupIds, mergedBackupId);
            LOG.info("backupsToDelete: " + backupsToDelete);
            updateBackupManifest(tmpBackupDir.getParent().toString(), mergedBackupId, backupsToDelete);


            // Copy meta files back from tmp to backup dir
            copyMetaData(tmpBackupDir, backupDirPath);
            // Delete tmp dir (Rename back during repair)
            if (!fsIn.delete(tmpBackupDir, true)) {
                // WARN and ignore
                LOG.warn("Could not delete tmp dir: " + tmpBackupDir);
            }
            // Delete old data
            deleteBackupImages(backupsToDelete, conn, backupRoot);
            // Finish merge session
            table.finishMergeOperation();
            // Release lock
            table.finishBackupExclusiveOperation();
        } catch (RuntimeException e) {

            throw e;
        } catch (Exception e) {
            LOG.error(e.toString(), e);
            if (!finishedTables) {
                // cleanup bulk directories and finish merge
                // merge MUST be repeated (no need for repair)
                cleanupBulkLoadDirs(toPathList(processedTableList));
                table.finishMergeOperation();
                table.finishBackupExclusiveOperation();
                throw new IOException("Backup merge operation failed, you should try it again", e);
            } else {
                // backup repair must be run
                throw new IOException(
                        "Backup merge operation failed, run backup repair tool to restore system's integrity",
                        e);
            }
        } finally {
            table.close();
            conn.close();
        }
    }

    /**
     * Copy meta data to of a backup session
     *
     * @param tmpBackupDir  temp backup directory, where meta is locaed
     * @param backupDirPath new path for backup
     * @throws IOException exception
     */
    protected void copyMetaData(Path tmpBackupDir, Path backupDirPath)
            throws IOException {

        FileSystem fs = backupDirPath.getFileSystem(conf);

        RemoteIterator<LocatedFileStatus> it = fs.listFiles(tmpBackupDir, true);
        List<Path> toKeep = new ArrayList<Path>();
        while (it.hasNext()) {
            Path p = it.next().getPath();
            if (fs.isDirectory(p)) {
                continue;
            }
            // Keep meta
            String fileName = p.toString();
            if (fileName.indexOf(TABLEINFO_DIR) > 0
                    || fileName.indexOf(HRegionFileSystem.REGION_INFO_FILE) > 0) {
                toKeep.add(p);
            }
        }
        // Copy meta to destination
        for (Path p : toKeep) {
            Path newPath = convertToDest(p, backupDirPath);
            copyFile(fs, p, newPath);
        }
    }

    /**
     * Copy file in DFS from p to newPath
     *
     * @param fs      file system
     * @param p       old path
     * @param newPath new path
     * @throws IOException exception
     */
    protected void copyFile(FileSystem fs, Path p, Path newPath) throws IOException {
        File f = File.createTempFile("data", "meta");
        Path localPath = new Path(f.getAbsolutePath());
        fs.copyToLocalFile(p, localPath);
        fs.copyFromLocalFile(localPath, newPath);
        boolean exists = fs.exists(newPath);
        if (!exists) {
            throw new IOException("Failed to copy meta file to: " + newPath);
        }
    }

    /**
     * Converts path before copying
     *
     * @param p             path
     * @param backupDirPath backup root
     * @return converted path
     */
    protected Path convertToDest(Path p, Path backupDirPath) {
        String backupId = backupDirPath.getName();
        Stack<String> stack = new Stack<String>();
        String name = null;
        while (true) {
            name = p.getName();
            if (!name.equals(backupId)) {
                stack.push(name);
                p = p.getParent();
            } else {
                break;
            }
        }
        Path newPath = new Path(backupDirPath.toString());
        while (!stack.isEmpty()) {
            newPath = new Path(newPath, stack.pop());
        }
        return newPath;
    }

    protected List<Path> toPathList(List<Pair<TableName, Path>> processedTableList) {
        ArrayList<Path> list = new ArrayList<>();
        for (Pair<TableName, Path> p : processedTableList) {
            list.add(p.getSecond());
        }
        return list;
    }

    protected List<TableName> toTableNameList(List<Pair<TableName, Path>> processedTableList) {
        ArrayList<TableName> list = new ArrayList<>();
        for (Pair<TableName, Path> p : processedTableList) {
            list.add(p.getFirst());
        }
        return list;
    }

    protected void cleanupBulkLoadDirs(List<Path> pathList) throws IOException {
        for (Path path : pathList) {
            if (!path.getFileSystem(conf).delete(path, true)) {
                LOG.warn("Can't delete " + path);
            }
        }
    }

    protected void updateBackupManifest(String backupRoot, String mergedBackupId,
                                        List<String> backupsToDelete) throws IllegalArgumentException, IOException {
        BackupManifest manifest =
                HBackupFileSystem.getManifest(conf, new Path(backupRoot), mergedBackupId);
        manifest.getBackupImage().removeAncestors(backupsToDelete);

        // save back
        manifest.store(conf);
    }

    protected void deleteBackupImages(List<String> backupIds, Connection conn,
                                      String backupRoot) throws IOException {
        // Delete from backup system table
        try (BackupSystemTable table = new BackupSystemTable(conn)) {
            for (String backupId : backupIds) {
                table.deleteBackupInfo(backupId);
            }
        }

        // Delete from file system
        for (String backupId : backupIds) {
            Path backupDirPath = HBackupFileSystem.getBackupPath(backupRoot, backupId);
            FileSystem fs = backupDirPath.getFileSystem(conf);
            if (!fs.delete(backupDirPath, true)) {
                LOG.warn("Could not delete " + backupDirPath);
            }
        }
    }

    protected List<String> getBackupIdsToDelete(String[] backupIds, String mergedBackupId) {
        List<String> list = new ArrayList<>();
        for (String id : backupIds) {
            if (id.equals(mergedBackupId)) {
                continue;
            }
            list.add(id);
        }
        return list;
    }

    protected void moveData(String backupRoot, Path bulkOutputPath,
                            TableName tableName, String mergedBackupId) throws Exception {
        Path dest =
                new Path(HBackupFileSystem.getTableBackupDir(backupRoot, mergedBackupId, tableName));

        FileSystem bulkOutputFs = bulkOutputPath.getFileSystem(conf);
        FileSystem destFs = dest.getFileSystem(conf);

        FileStatus[] fsts = bulkOutputFs.listStatus(bulkOutputPath);
        for (FileStatus fst : fsts) {
            if (fst.isDirectory()) {
                String family = fst.getPath().getName();
                Path newDst = new Path(dest, family);
                if (destFs.exists(newDst)) {
                    if (!destFs.delete(newDst, true)) {
                        throw new IOException("failed to delete :" + newDst);
                    }
                } else {
                    destFs.mkdirs(dest);
                }

                DistCpOptions distCpOptions = new DistCpOptions(Arrays.asList(fst.getPath()), dest);
                DistCp ds = new DistCp(getConf(), distCpOptions);
                Job res = ds.execute();

                LOG.info("Deleting directory: " + fst.getPath());
                bulkOutputFs.delete(fst.getPath(), true);
            }
        }
    }

    protected TableName[] getTableNamesInBackupImages(String[] backupIds) throws IOException {
        Set<TableName> allSet = new HashSet<>();

        try (Connection conn = ConnectionFactory.createConnection(conf);
             BackupSystemTable table = new BackupSystemTable(conn)) {
            for (String backupId : backupIds) {
                BackupInfo bInfo = table.readBackupInfo(backupId);

                allSet.addAll(bInfo.getTableNames());
            }
        }

        TableName[] ret = new TableName[allSet.size()];
        return allSet.toArray(ret);
    }

    protected Path[] findInputDirectories(String backupRoot, TableName tableName,
                                          String[] backupIds) throws IOException {
        List<Path> dirs = new ArrayList<>();

        for (String backupId : backupIds) {
            Path fileBackupDirPath =
                    new Path(HBackupFileSystem.getTableBackupDir(backupRoot, backupId, tableName));

            FileSystem fs = new Path(backupRoot).getFileSystem(conf);
            if (fs.exists(fileBackupDirPath)) {
                dirs.add(fileBackupDirPath);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("File: " + fileBackupDirPath + " does not exist.");
                }
            }
        }
        Path[] ret = new Path[dirs.size()];
        return dirs.toArray(ret);
    }
}
