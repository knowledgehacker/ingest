package ingest;

/**
 * Created by mlin on 10/9/14.
 */
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import java.security.PrivilegedExceptionAction;

public class Worker extends Thread {
    private final Configuration _conf;
    private final FileSystemOps _fsOps;
    private final List<MyPath> _logFiles;

    private final String _workDir;
    private final String _destinationDir;

    public Worker(Configuration conf, FileSystemOps fsOps, List<MyPath> logFiles,
        String workDir, String destinationDir) {
        _conf = conf;
        _fsOps = fsOps;
        _logFiles = logFiles;

        _workDir = workDir;
        _destinationDir = destinationDir;
    }

    public void run() {
        System.out.println("Thread " + Thread.currentThread().getName() + " running...");
        for(MyPath logFile: _logFiles)
            System.out.println(logFile.getPath());
        System.out.println("*****");
		/*
		try {
			UserGroupInformation ugi = UserGroupInformation.createRemoteUser("hdfs");
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
				public Void run() throws Exception {
					copy();
					return null;
				}
			});
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		*/
		
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
