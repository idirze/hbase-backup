package org.apache.hadoop.hbase.backup.hbase1_2_1.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;

import java.io.IOException;
import java.util.List;

public class SafeRestoreTool {

    protected static final Log LOG = LogFactory.getLog(SafeRestoreTool.class);

    private static String SAFE_RESTORE_SNAPSHOT_SUFFIX = "_safe_restore_hbase1_2_1";


    public static boolean recreateSnapshot(Connection conn, final TableName tableName)
            throws IOException, IllegalArgumentException {

        String snapshotName = tableName.getNameAsString().replace(":", "_") + SAFE_RESTORE_SNAPSHOT_SUFFIX;

        try (Admin admin = conn.getAdmin()) {

           /* if (snapshotExists(conn, snapshotName)) {
                LOG.info("Deleting existing snapshot " + snapshotName + " for the table " + tableName.getNameAsString());
                admin.deleteSnapshot(snapshotName);
            }
            LOG.info("Creating snapshot for the table " + tableName.getNameAsString());
            admin.snapshot(snapshotName, tableName);
            */

            if (!snapshotExists(conn, snapshotName)) {
                LOG.info("Creating snapshot for the table " + tableName.getNameAsString());
                admin.snapshot(snapshotName, tableName);
            }
        }

        return snapshotExists(conn, snapshotName);

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
