/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.backup.hbase1_2_1.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.backup.hbase1_2_1.impl.BackupManager;
import org.apache.hadoop.hbase.backup.hbase1_2_1.impl.BackupSystemTable;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.master.cleaner.BaseLogCleanerDelegate;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of a log cleaner that checks if a log is still scheduled for
 * incremental backup before deleting it when its TTL is over.
 */
@InterfaceStability.Evolving
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class BackupLogCleaner extends BaseLogCleanerDelegate {
    private static final Log LOG = LogFactory.getLog(BackupLogCleaner.class);

    private boolean stopped = false;

    public BackupLogCleaner() {
    }

    @Override
    public Iterable<FileStatus> getDeletableFiles(Iterable<FileStatus> files) {
        // all members of this class are null if backup is disabled,
        // so we cannot filter the files
        if (this.getConf() == null || !BackupManager.isBackupEnabled(getConf())) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Backup is not enabled. Check your "
                        + HConstants.BACKUP_ENABLE_KEY + " setting");
            }
            return files;
        }

        List<FileStatus> list = new ArrayList<>();
        // TODO: LogCleaners do not have a way to get the Connection from Master. We should find a
        // way to pass it down here, so that this connection is not re-created every time.
        // It is expensive
        try (final Connection conn = ConnectionFactory.createConnection(getConf());
             final BackupSystemTable table = new BackupSystemTable(conn)) {
            // If we do not have recorded backup sessions
            if (!table.hasBackupSessions()) {
                LOG.debug("BackupLogCleaner has no backup sessions");
                return files;
            }

            for (FileStatus file : files) {
                String wal = file.getPath().toString();
                boolean logInSystemTable = table.isWALFileDeletable(wal);
                if (LOG.isDebugEnabled()) {
                    if (logInSystemTable) {
                        LOG.debug("Found log file in hbase:backup, deleting: " + wal);
                        list.add(file);
                    } else {
                        LOG.debug("Didn't find this log in hbase:backup, keeping: " + wal);
                    }
                }
            }
            return list;
        } catch (IOException e) {
            LOG.error("Failed to get hbase:backup table, therefore will keep all files", e);
            // nothing to delete
            return Collections.emptyList();
        }
    }

    @Override
    public void setConf(Configuration config) {
        super.setConf(config);
        // If backup is disabled, keep all members null
        if (!config.getBoolean(HConstants.BACKUP_ENABLE_KEY, HConstants.BACKUP_ENABLE_DEFAULT)) {
            LOG.warn("Backup is disabled - allowing all wals to be deleted");
            return;
        }
    }

    @Override
    public void stop(String why) {
        if (this.stopped) {
            return;
        }
        this.stopped = true;
        LOG.info("Stopping BackupLogCleaner");
    }

    @Override
    public boolean isStopped() {
        return this.stopped;
    }

}
