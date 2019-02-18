package org.apache.hadoop.hbase.backup.hbase1_2_1.mapreduce;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.hadoop.hbase.backup.hbase1_2_1.mapreduce.MapReduceBackupCopyJob.NUMBER_OF_LEVELS_TO_PRESERVE_KEY;

public class MapReduceMoveMergedHfilesJob extends DistCp {

    public static final Logger LOG = LoggerFactory.getLogger(MapReduceMoveMergedHfilesJob.class);

    public MapReduceMoveMergedHfilesJob(Configuration conf, DistCpOptions options) throws Exception {
        super(conf, options);
    }

    @Override
    public Job execute() throws Exception {

        // reflection preparation for private methods and fields
        Class<?> classDistCp = org.apache.hadoop.tools.DistCp.class;
        Method methodCleanup = classDistCp.getDeclaredMethod("cleanup");

        Field fieldInputOptions = getInputOptionsField(classDistCp);
        Field fieldSubmitted = classDistCp.getDeclaredField("submitted");

        methodCleanup.setAccessible(true);
        fieldInputOptions.setAccessible(true);
        fieldSubmitted.setAccessible(true);

        // execute() logic starts here
        assert fieldInputOptions.get(this) != null;

        Job job = super.execute();

        String jobID = job.getJobID().toString();
        job.getConfiguration().set(DistCpConstants.CONF_LABEL_DISTCP_JOB_ID, jobID);

        LOG.debug("DistCp job-id: " + jobID + " completed: " + job.isComplete() + " "
                + job.isSuccessful());
        Counters ctrs = job.getCounters();
        LOG.debug(Objects.toString(ctrs));
        if (job.isComplete() && !job.isSuccessful()) {
            throw new Exception("DistCp job-id: " + jobID + " failed");
        }

        return job;
    }

    private Field getInputOptionsField(Class<?> classDistCp) throws IOException {
        Field f = null;
        try {
            f = classDistCp.getDeclaredField("inputOptions");
        } catch (Exception e) {
            // Haddop 3
            try {
                f = classDistCp.getDeclaredField("context");
            } catch (NoSuchFieldException | SecurityException e1) {
                throw new IOException(e1);
            }
        }
        return f;
    }

    @SuppressWarnings("unchecked")
    private List<Path> getSourcePaths(Field fieldInputOptions) throws IOException {
        Object options;
        try {
            options = fieldInputOptions.get(this);
            if (options instanceof DistCpOptions) {
                return ((DistCpOptions) options).getSourcePaths();
            } else {
                // Hadoop 3
                Class<?> classContext = Class.forName("org.apache.hadoop.tools.DistCpContext");
                Method methodGetSourcePaths = classContext.getDeclaredMethod("getSourcePaths");
                methodGetSourcePaths.setAccessible(true);

                return (List<Path>) methodGetSourcePaths.invoke(options);
            }
        } catch (IllegalArgumentException | IllegalAccessException |
                ClassNotFoundException | NoSuchMethodException |
                SecurityException | InvocationTargetException e) {
            throw new IOException(e);
        }

    }


    @Override
    protected Path createInputFileListing(Job job) throws IOException {

        long totalBytesExpected = 0;
        int totalRecords = 0;
        Path fileListingPath = getFileListingPath();
        try (SequenceFile.Writer writer = getWriter(fileListingPath)) {
            List<Path> srcFiles = getSourceFiles();
            if (srcFiles.size() == 0) {
                return fileListingPath;
            }
            totalRecords = srcFiles.size();
            FileSystem fs = srcFiles.get(0).getFileSystem(getConf());
            for (Path path : srcFiles) {
                FileStatus fst = fs.getFileStatus(path);
                totalBytesExpected += fst.getLen();
                Text key = getKey(path);
                writer.append(key, new CopyListingFileStatus(fst));
            }

            writer.close();

            // update jobs configuration

            Configuration cfg = job.getConfiguration();
            cfg.setLong(DistCpConstants.CONF_LABEL_TOTAL_BYTES_TO_BE_COPIED, totalBytesExpected);
            cfg.set(DistCpConstants.CONF_LABEL_LISTING_FILE_PATH, fileListingPath.toString());
            cfg.setLong(DistCpConstants.CONF_LABEL_TOTAL_NUMBER_OF_RECORDS, totalRecords);
        } catch (NoSuchFieldException | SecurityException | IllegalArgumentException e) {
            throw new IOException(e);
        }
        return fileListingPath;
    }


    private Text getKey(Path path) {
        int level = getConf().getInt(NUMBER_OF_LEVELS_TO_PRESERVE_KEY, 1);
        int count = 0;
        String relPath = "";
        while (count++ < level) {
            relPath = Path.SEPARATOR + path.getName() + relPath;
            path = path.getParent();
        }
        return new Text(relPath);
    }

    private List<Path> getSourceFiles() throws NoSuchFieldException, SecurityException,
            IllegalArgumentException, IOException {
        Field options = null;
        try {
            options = DistCp.class.getDeclaredField("inputOptions");
        } catch (NoSuchFieldException | SecurityException e) {
            options = DistCp.class.getDeclaredField("context");
        }
        options.setAccessible(true);
        return getSourcePaths(options);
    }


    private SequenceFile.Writer getWriter(Path pathToListFile) throws IOException {
        FileSystem fs = pathToListFile.getFileSystem(getConf());
        fs.delete(pathToListFile, false);
        return SequenceFile.createWriter(getConf(), SequenceFile.Writer.file(pathToListFile),
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(CopyListingFileStatus.class),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
    }

    public int copy(String[] files, String backupDest) throws Exception {

        MapReduceMoveMergedHfilesJob distcp =
                new MapReduceMoveMergedHfilesJob(new Configuration(getConf()), null);

        String[] options = new String[files.length + 2];
        options[0] = "-async"; // run DisCp in async mode
        System.arraycopy(files, 0, options, 1, files.length);
        options[options.length-1] = backupDest;

        LOG.info("DistCp options: " + Arrays.toString(options));
        Path dest = new Path(options[options.length - 1]);
        FileSystem destfs = dest.getFileSystem(getConf());

        if (!destfs.exists(dest)) {
            destfs.mkdirs(dest);
        }

        int res = distcp.run(options);

        if (res != 0) {
            LOG.error("Copy HFile files failed with return code: " + res + ".");
            throw new IOException("Failed copy from " + StringUtils.join(files, ',')
                    + " to " + backupDest);
        }
        LOG.debug("Copy HFiles from " + StringUtils.join(files, ',')
                + " to " + backupDest + " finished.");

        return res;

    }

}
