package org.apache.hadoop.hbase.backup.hbase1_2_1.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.directory.api.util.Strings;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class SafeRestoreTool {

    protected static final Log LOG = LogFactory.getLog(SafeRestoreTool.class);

    private static String SAFE_RESTORE_SNAPSHOT_SUFFIX = "_hbase1_2_1_safe_restore";
    private static String SAFE_FIRST_RESTORE_SNAPSHOT_SUFFIX = "_hbase1_2_1_safe_initial_restore";
    private static String SAFE_FIRST_BACKUP_SNAPSHOT_SUFFIX = "_hbase1_2_1_safe_initial_backup";
    private final static long TABLE_AVAILABILITY_WAIT_TIME = 180000;




    public static void snapshotOnBackup(Connection conn, final TableName tableName) throws IOException {
        String firstSnapshotBackupName = tableName.getNameAsString().replace(":", "_")
                + SAFE_FIRST_BACKUP_SNAPSHOT_SUFFIX;

        try (Admin admin = conn.getAdmin()) {

            /* Test Only */
            Optional<String> createFirstSnapshotBackup = Optional.ofNullable(System.getProperty("create.initial.snapshot.on.backup"));

            Optional<String> deleteFirstSnapshotBackup = Optional.ofNullable(System.getProperty("delete.initial.snapshot.on.backup"));

            if (deleteFirstSnapshotBackup.orElse("no")
                    .equalsIgnoreCase("yes")
                    && snapshotExists(conn, firstSnapshotBackupName)) {
                LOG.info("Deleting first snapshot restore: " + firstSnapshotBackupName + " for the table: " + tableName.getNameAsString());
                admin.deleteSnapshot(firstSnapshotBackupName);
            }

            if (createFirstSnapshotBackup.orElse("no")
                    .equalsIgnoreCase("yes")
                    && !snapshotExists(conn, firstSnapshotBackupName)) {
                if (admin.tableExists(tableName)) {
                    LOG.info("Creating first snapshot restore : " + firstSnapshotBackupName + " for the table: " + tableName.getNameAsString());
                    admin.snapshot(firstSnapshotBackupName, tableName);
                }
            }
        }
    }

    public static boolean snapshot(Connection conn, final TableName tableName)
            throws IOException, IllegalArgumentException {

        String snapshotName = snapshotName(tableName);
        String firstSnapshotRestoreName = tableName.getNameAsString().replace(":", "_")
                + SAFE_FIRST_RESTORE_SNAPSHOT_SUFFIX;

        try (Admin admin = conn.getAdmin()) {

            /* Test Only */
            Optional<String> createFirstSnapshotRestore = Optional.ofNullable(System.getProperty("create.initial.snapshot.on.restore"));
            Optional<String> deleteFirstSnapshotRestore = Optional.ofNullable(System.getProperty("delete.initial.snapshot.on.restore"));

            if (deleteFirstSnapshotRestore.orElse("no")
                    .equalsIgnoreCase("yes")
                    && snapshotExists(conn, firstSnapshotRestoreName)) {
                LOG.info("Deleting first snapshot restore: " + firstSnapshotRestoreName + " for the table: " + tableName.getNameAsString());
                admin.deleteSnapshot(firstSnapshotRestoreName);
            }

            if (createFirstSnapshotRestore.orElse("no")
                    .equalsIgnoreCase("yes")
                    && !snapshotExists(conn, firstSnapshotRestoreName)) {
                if (admin.tableExists(tableName)) {
                    LOG.info("Creating first snapshot restore : " + firstSnapshotRestoreName + " for the table: " + tableName.getNameAsString());
                    admin.snapshot(firstSnapshotRestoreName, tableName);
                }
            }

            /* ---- */


            if (!snapshotExists(conn, snapshotName)) {
                if (admin.tableExists(tableName)) {
                    LOG.info("Creating snapshot for the table " + tableName.getNameAsString());
                    admin.snapshot(snapshotName, tableName);
                }
            }
        }

        return snapshotExists(conn, snapshotName);

    }

    public static void deleteSnapshot(Connection conn, final TableName tableName) throws Exception {

        String snapshotName = snapshotName(tableName);

        try (Admin admin = conn.getAdmin()) {
            if (snapshotExists(conn, snapshotName)) {
                LOG.info("Rollback: Deleting snapshot : " + snapshotName(tableName));
                admin.deleteSnapshot(snapshotName);
            }
        }
    }

    public static void deleteSnapshot(Connection conn, final TableName tableName, boolean silent) throws Exception {

        String snapshotName = snapshotName(tableName);

        try {
            deleteSnapshot(conn, tableName);
        } catch (Exception e) {
            LOG.error("Error deleting table: " + tableName.getNameAsString() + " from snapshot: " + snapshotName);
            if (!silent) {
                throw e;
            }
        }
    }

    public static String snapshotName(TableName tableName) {
        return tableName.getNameAsString().replace(":", "_") + SAFE_RESTORE_SNAPSHOT_SUFFIX;
    }

    public static boolean restoreSnapshot(Connection conn, final TableName tableName)
            throws Exception {

        LOG.info("Rollback: Restoring the table: " + tableName.getNameAsString() + " from the saved snapshot: " + snapshotName(tableName));

        String snapshotName = snapshotName(tableName);

        try (Admin admin = conn.getAdmin()) {

            if (snapshotExists(conn, snapshotName)) {
                LOG.info("Restoring snapshot for the table " + tableName.getNameAsString());
                admin.disableTable(tableName);
                checkDisabled(conn, tableName);
                admin.restoreSnapshot(snapshotName);
                admin.enableTable(tableName);
                checkAvailable(conn, tableName);
            }
        }

        return snapshotExists(conn, snapshotName);

    }

    public static boolean checkAvailable(Connection conn,
                                         TableName table) throws Exception {

        try (Admin admin = conn.getAdmin()) {

            long startTime = EnvironmentEdgeManager.currentTime();
            while (!admin.isTableAvailable(table)) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                if (EnvironmentEdgeManager.currentTime() - startTime > TABLE_AVAILABILITY_WAIT_TIME) {
                    throw new IOException("Time out " + TABLE_AVAILABILITY_WAIT_TIME + "ms expired, table "
                            + table + " is still not available");
                }
            }
        }

        return true;

    }

    public static boolean checkDisabled(Connection conn,
                                        TableName table) throws Exception {

        try (Admin admin = conn.getAdmin()) {

            long startTime = EnvironmentEdgeManager.currentTime();
            while (!admin.isTableDisabled(table)) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
                if (EnvironmentEdgeManager.currentTime() - startTime > TABLE_AVAILABILITY_WAIT_TIME) {
                    throw new IOException("Time out " + TABLE_AVAILABILITY_WAIT_TIME + "ms expired, table "
                            + table + " is still not disabled");
                }
            }
        }

        return true;

    }

    public static boolean snapshotExists(Connection conn, final String snapshotName) throws IOException {
        try (Admin admin = conn.getAdmin()) {
            List<SnapshotDescription> snapshots = admin.listSnapshots(snapshotName);
            return !snapshots.isEmpty();
        }
    }

    public static void createNamespaceIfNotExists(Connection conn, final String namespaceName) throws IOException {
        try (Admin admin = conn.getAdmin()) {
            NamespaceDescriptor ns = NamespaceDescriptor.create(namespaceName).build();
            NamespaceDescriptor[] list = admin.listNamespaceDescriptors();
            boolean exists = false;
            for (NamespaceDescriptor nsd : list) {
                if (nsd.getName().equals(ns.getName())) {
                    exists = true;
                    break;
                }
            }
            if (!exists) {
                LOG.info("Creating namespace: " + ns);
                admin.createNamespace(ns);
            }
        }
    }

}
