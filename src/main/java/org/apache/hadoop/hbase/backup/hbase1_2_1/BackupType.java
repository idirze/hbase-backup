package org.apache.hadoop.hbase.backup.hbase1_2_1;


import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public enum BackupType {
    FULL,
    INCREMENTAL;

    BackupType() {
    }
}