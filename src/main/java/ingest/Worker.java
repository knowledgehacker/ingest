/**
 * Created by mlin on 10/9/14.
 */
package ingest;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class Worker extends Thread {
	private final Logger LOG = LoggerFactory.getLogger(Worker.class);

    private final Configuration _conf;
    private final List<MyPath> _logFiles;
    private final String _destinationDir;
    private final boolean _verify;

    private final FileSystemOps _fsOps;

	private final CountDownLatch _startSignal;
	private final CountDownLatch _doneSignal;

    public Worker(Configuration conf, List<MyPath> logFiles, String destinationDir, boolean verify,
		CountDownLatch startSignal, CountDownLatch doneSignal) {
        _conf = conf;
        _logFiles = logFiles;
        _destinationDir = destinationDir;
	    _verify = verify;
        
		_fsOps = new FileSystemOps(4096);

		_startSignal = startSignal;
		_doneSignal = doneSignal;
    }

    public void run() {
		try {
			_startSignal.await();
		} catch (InterruptedException ie) {
		}

        for(MyPath logFile: _logFiles) {
            try {
                if (!_fsOps.exists(logFile)) {
                    LOG.error("File " + logFile.getPath() + " doesn't exist.");
				}
            } catch (IOException ioe) {
                LOG.error("Check existence of file " + logFile.getPath() + " failed - " + ioe.toString());
            }

            MyPath destinationFile = null;
            try {
                destinationFile = new MyPath(_conf, new Path(_destinationDir + "/" + logFile.getPath().getName()));
                _fsOps.copy(logFile, destinationFile, _verify);
            } catch (IOException ioe) {
                LOG.error("Copy file " + logFile.getPath() + " to " + destinationFile.getPath() + " failed - "
                        + ioe.toString());
            }
        }

		_doneSignal.countDown();
    }
}
