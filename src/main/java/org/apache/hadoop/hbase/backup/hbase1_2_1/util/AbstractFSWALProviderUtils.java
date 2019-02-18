package org.apache.hadoop.hbase.backup.hbase1_2_1.util;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractFSWALProviderUtils {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractFSWALProviderUtils.class);

    // Implementation details that currently leak in tests or elsewhere follow
    /** File Extension used while splitting an WAL into regions (HBASE-2312) */
    public static final String SPLITTING_EXT = "-splitting";

    /** The hbase:meta region's WAL filename extension */
    @VisibleForTesting
    public static final String META_WAL_PROVIDER_ID = ".meta";

    public static boolean isArchivedLogFile(Path p) {
        String oldLog = Path.SEPARATOR + HConstants.HREGION_OLDLOGDIR_NAME + Path.SEPARATOR;
        return p.toString().contains(oldLog);
    }

    /**
     * This function returns region server name from a log file name which is in one of the following
     * formats:
     * <ul>
     * <li>hdfs://&lt;name node&gt;/hbase/.logs/&lt;server name&gt;-splitting/...</li>
     * <li>hdfs://&lt;name node&gt;/hbase/.logs/&lt;server name&gt;/...</li>
     * </ul>
     * @return null if the passed in logFile isn't a valid WAL file path
     */
    public static ServerName getServerNameFromWALDirectoryName(Path logFile) {
        String logDirName = logFile.getParent().getName();
        // We were passed the directory and not a file in it.
        if (logDirName.equals(HConstants.HREGION_LOGDIR_NAME)) {
            logDirName = logFile.getName();
        }
        ServerName serverName = null;
        if (logDirName.endsWith(SPLITTING_EXT)) {
            logDirName = logDirName.substring(0, logDirName.length() - SPLITTING_EXT.length());
        }
        try {
            serverName = ServerName.parseServerName(logDirName);
        } catch (IllegalArgumentException | IllegalStateException ex) {
            serverName = null;
            LOG.warn("Cannot parse a server name from path=" + logFile + "; " + ex.getMessage());
        }
        if (serverName != null && serverName.getStartcode() < 0) {
            LOG.warn("Invalid log file path=" + logFile);
            serverName = null;
        }
        return serverName;
    }

    public static boolean isMetaFile(Path p) {
        return isMetaFile(p.getName());
    }

    public static boolean isMetaFile(String p) {
        if (p != null && p.endsWith(META_WAL_PROVIDER_ID)) {
            return true;
        }
        return false;
    }


}
