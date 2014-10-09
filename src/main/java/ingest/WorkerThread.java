package ingest;

/**
 * Created by mlin on 10/9/14.
 */
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class WorkerThread extends Thread {
    private final Configuration _conf;
    private final FileSystemOps _fsOps;
    private final List<MyPath> _logFiles;

    private final String _workDir;
    private final String _destinationDir;

    public WorkerThread(Configuration conf, FileSystemOps fsOps, List<MyPath> logFiles,
        String workDir, String destinationDir) {
        _conf = conf;
        _fsOps = fsOps;
        _logFiles = logFiles;

        _workDir = workDir;
        _destinationDir = destinationDir;
    }

    public void run() {
        copy();
    }

    public void copy() {
        for(MyPath logFile: _logFiles) {
            Path myPath = logFile.getPath();
            try {
                MyPath workFile = new MyPath(_conf, new Path(_workDir + "/" + myPath.getName()));
                MyPath destinationFile = new MyPath(_conf, new Path(_destinationDir + "/" + myPath.getName()));
                _fsOps.copy(workFile, destinationFile);
            } catch (IOException ioe) {
                throw new RuntimeException(ioe.toString());
            }
        }
    }
}
