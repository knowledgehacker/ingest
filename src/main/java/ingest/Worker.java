package ingest;

/**
 * Created by mlin on 10/9/14.
 */
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class Worker extends Thread {
    private final Configuration _conf;
    private final List<MyPath> _logFiles;
    private final String _destinationDir;
    private final boolean _verify;

    private final FileSystemOps _fsOps;

    /*
    public Worker(Configuration conf, List<MyPath> logFiles, String destinationDir, boolean verify) {
        _conf = conf;
        _logFiles = logFiles;

        _destinationDir = destinationDir;
	    _verify = verify;
        
		_fsOps = new FileSystemOps(4096);
    }
    */

    public Worker(Configuration conf, String destinationDir, boolean verify) {
        _conf = conf;
        _logFiles = new ArrayList<MyPath>();
        _destinationDir = destinationDir;
        _verify = verify;

        _fsOps = new FileSystemOps(4096);
    }

    public void assign(List<MyPath> logFiles) {
        _logFiles.addAll(logFiles);
    }

    public void run() {
        long start = System.currentTimeMillis();
        for(MyPath logFile: _logFiles) {
            MyPath destinationFile = null;
            try {
                destinationFile = new MyPath(_conf, new Path(_destinationDir + "/" + logFile.getPath().getName()));
                _fsOps.copy(logFile, destinationFile, _verify);
            } catch (IOException ioe) {
                throw new RuntimeException("Copy file " + logFile.getPath() + " to " + destinationFile.getPath() + " failed - "
                        + ioe.toString());
            }
        }
        System.out.println(Thread.currentThread().getName() + " - copying files takes " + (System.currentTimeMillis() - start) + " milliseconds.");
    }
}
