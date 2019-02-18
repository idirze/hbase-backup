package org.apache.hadoop.hbase.backup.hbase1_2_1.regionserver;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoordinatedStateManagerFactory;
import org.apache.hadoop.hbase.backup.hbase1_2_1.BackupRestoreConstants;
import org.apache.hadoop.hbase.backup.hbase1_2_1.LogRollMasterProcedureManager;
import org.apache.hadoop.hbase.backup.hbase1_2_1.impl.BackupManager;
import org.apache.hadoop.hbase.coordination.BaseCoordinatedStateManager;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.procedure.*;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

import java.io.IOException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * This manager class handles the work dealing with backup for a {@link HRegionServer}.
 * <p>
 * This provides the mechanism necessary to kick off a backup specific {@link Subprocedure} that is
 * responsible by this region server. If any failures occur with the subprocedure, the manager's
 * procedure member notifies the procedure coordinator to abort all others.
 * <p>
 * On startup, requires {@link #start()} to be called.
 * <p>
 * On shutdown, requires org.apache.hadoop.hbase.procedure.ProcedureMember.close() to be
 * called
 */
public class LogRollRegionServerProcedureManager extends RegionServerProcedureManager {

    private static final Log LOG = LogFactory.getLog(LogRollRegionServerProcedureManager.class);

    /**
     * Conf key for number of request threads to start backup on regionservers
     */
    public static final String BACKUP_REQUEST_THREADS_KEY = "hbase.backup.region.pool.threads";
    /**
     * # of threads for backup work on the rs.
     */
    public static final int BACKUP_REQUEST_THREADS_DEFAULT = 10;

    public static final String BACKUP_TIMEOUT_MILLIS_KEY = "hbase.backup.timeout";
    public static final long BACKUP_TIMEOUT_MILLIS_DEFAULT = 60000;

    /**
     * Conf key for millis between checks to see if backup work completed or if there are errors
     */
    public static final String BACKUP_REQUEST_WAKE_MILLIS_KEY = "hbase.backup.region.wakefrequency";
    /**
     * Default amount of time to check for errors while regions finish backup work
     */
    private static final long BACKUP_REQUEST_WAKE_MILLIS_DEFAULT = 500;

    private RegionServerServices rss;
    private ProcedureMemberRpcs memberRpcs;
    private ProcedureMember member;
    private boolean started = false;

    /**
     * Create a default backup procedure manager
     */
    public LogRollRegionServerProcedureManager() {
    }

    /**
     * Start accepting backup procedure requests.
     */
    @Override
    public void start() {
        if (!BackupManager.isBackupEnabled(rss.getConfiguration())) {
            LOG.warn("Backup is not enabled. Check your " + BackupRestoreConstants.BACKUP_ENABLE_KEY
                    + " setting");
            return;
        }
        this.memberRpcs.start(rss.getServerName().toString(), member);
        started = true;
        LOG.info("Started region server backup manager.");
    }

    /**
     * Close <tt>this</tt> and all running backup procedure tasks
     * @param force forcefully stop all running tasks
     * @throws IOException exception
     */
    @Override
    public void stop(boolean force) throws IOException {
        if (!started) {
            return;
        }
        String mode = force ? "abruptly" : "gracefully";
        LOG.info("Stopping RegionServerBackupManager " + mode + ".");

        try {
            this.member.close();
        } finally {
            this.memberRpcs.close();
        }
    }

    /**
     * If in a running state, creates the specified subprocedure for handling a backup procedure.
     * @return Subprocedure to submit to the ProcedureMemeber.
     */
    public Subprocedure buildSubprocedure(byte[] data) {
        // don't run a backup if the parent is stop(ping)
        if (rss.isStopping() || rss.isStopped()) {
            throw new IllegalStateException("Can't start backup procedure on RS: " + rss.getServerName()
                    + ", because stopping/stopped!");
        }

        LOG.info("Attempting to run a roll log procedure for backup.");
        ForeignExceptionDispatcher errorDispatcher = new ForeignExceptionDispatcher();
        Configuration conf = rss.getConfiguration();
        long timeoutMillis = conf.getLong(BACKUP_TIMEOUT_MILLIS_KEY, BACKUP_TIMEOUT_MILLIS_DEFAULT);
        long wakeMillis =
                conf.getLong(BACKUP_REQUEST_WAKE_MILLIS_KEY, BACKUP_REQUEST_WAKE_MILLIS_DEFAULT);

        LogRollBackupSubprocedurePool taskManager =
                new LogRollBackupSubprocedurePool(rss.getServerName().toString(), conf);
        return new LogRollBackupSubprocedure(rss, member, errorDispatcher, wakeMillis, timeoutMillis,
                taskManager, data);
    }

    /**
     * Build the actual backup procedure runner that will do all the 'hard' work
     */
    public class BackupSubprocedureBuilder implements SubprocedureFactory {
        @Override
        public Subprocedure buildSubprocedure(String name, byte[] data) {
            return LogRollRegionServerProcedureManager.this.buildSubprocedure(data);
        }
    }

    @Override
    public void initialize(RegionServerServices rss) throws IOException {
        this.rss = rss;
        if (!BackupManager.isBackupEnabled(rss.getConfiguration())) {
            LOG.warn("Backup is not enabled. Check your " + BackupRestoreConstants.BACKUP_ENABLE_KEY
                    + " setting");
            return;
        }
        BaseCoordinatedStateManager coordManager =
                (BaseCoordinatedStateManager) CoordinatedStateManagerFactory.getCoordinatedStateManager(rss
                        .getConfiguration());
        coordManager.initialize(rss);
        this.memberRpcs =
                coordManager
                        .getProcedureMemberRpcs(LogRollMasterProcedureManager.ROLLLOG_PROCEDURE_SIGNATURE);

        // read in the backup handler configuration properties
        Configuration conf = rss.getConfiguration();
        long keepAlive = conf.getLong(BACKUP_TIMEOUT_MILLIS_KEY, BACKUP_TIMEOUT_MILLIS_DEFAULT);
        int opThreads = conf.getInt(BACKUP_REQUEST_THREADS_KEY, BACKUP_REQUEST_THREADS_DEFAULT);
        // create the actual cohort member
        ThreadPoolExecutor pool =
                ProcedureMember.defaultPool(rss.getServerName().toString(), opThreads, keepAlive);
        this.member = new ProcedureMember(memberRpcs, pool, new BackupSubprocedureBuilder());
    }

    @Override
    public String getProcedureSignature() {
        return "backup-proc";
    }
}
