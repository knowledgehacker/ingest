package ingest;

/**
 * Created by mlin on 10/9/14.
 */
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class DataIngester {
    private final Logger LOG = LoggerFactory.getLogger(DataIngester.class);

    private final String CONFIG_FILE = "/config.properties";

    private final String DATA_DATE_START = "data.date.start";
    private final String DATA_DATE_END = "data.date.end";
    private final String DATA_SOURCE_DIR = "data.source.dir";
    private final String DATA_DESTINATION_DIR = "data.destination.dir";
    private final String VERIFY = "verify";
    private final String THREAD_NUM = "thread.num";

    private String _dateStart;
    private String _dateEnd;
    private String _sourceDir;
    private String _destinationDir;
    private boolean _verify;
    private int _threadNum;

    private Configuration _conf;

    private Map<String, List<MyPath>> _binLogFiles;
    private List<MyPath> _ackLogFiles;

    public DataIngester() {

    }

    public void loadConfig() {
        InputStream is = DataIngester.class.getResourceAsStream(CONFIG_FILE);
        Properties properties = new Properties();
        try {
            properties.load(is);
        } catch (IOException ioe) {
            throw new RuntimeException("Load config file " + CONFIG_FILE + " failed.");
        }

        _dateStart = properties.getProperty(DATA_DATE_START);
        if(_dateStart.length() == 8)
            _dateStart += "0000";
        _dateEnd = properties.getProperty(DATA_DATE_END);
        if(_dateEnd.length() == 8)
            _dateEnd += "0000";
        _sourceDir = properties.getProperty(DATA_SOURCE_DIR);
        _destinationDir = properties.getProperty(DATA_DESTINATION_DIR);
        _verify = Boolean.parseBoolean(properties.getProperty(VERIFY));
        _threadNum = Integer.parseInt(properties.getProperty(THREAD_NUM));

        _conf = new Configuration();

        _binLogFiles = new HashMap<String, List<MyPath>>();
        _ackLogFiles = new ArrayList<MyPath>();
    }

    public void ingest() throws IOException {
        File dataSourceDir = null;
		try {
			dataSourceDir = new File(new URI(_sourceDir));
		} catch(URISyntaxException urise) {
			throw new RuntimeException("Open directory " + dataSourceDir + " failed - " + urise.toString());
        }

        File[] servers = dataSourceDir.listFiles();
        for(File server: servers) {
            File binaryDir = new File(server, "binary");
            File[] logFiles = binaryDir.listFiles();
            for(File logFile: logFiles) {
				String logFileName = logFile.getName();
                if(!(logFileName.startsWith("ack") || logFileName.startsWith("bin")))
                    continue;
                String[] parts = logFileName.substring(0, logFileName.indexOf('.')).split("-");
                String timestamp = parts[parts.length-1];
                if(timestamp.compareTo(_dateStart) >= 0 && timestamp.compareTo(_dateEnd) <= 0) {
                    String serverName = server.getName();
                    MyPath sourceFile = new MyPath(_conf, new Path(_sourceDir + "/" + serverName + "/" + binaryDir.getName() + "/" + logFile.getName()));
                    if(logFileName.startsWith("bin")) {
                        String dataCenterName = serverName.substring(0, serverName.indexOf("ads"));
                        List<MyPath> sourceFiles = _binLogFiles.get(dataCenterName);
                        if(sourceFiles == null)
                            sourceFiles = new ArrayList<MyPath>();
                         sourceFiles.add(sourceFile);
                        _binLogFiles.put(dataCenterName, sourceFiles);
                    } else
                        _ackLogFiles.add(sourceFile);
                }
            }
        }

		System.out.println("_threadNum: " + _threadNum + ", bin log file num: " + _binLogFiles.size());
        Worker[] binWorkers = new Worker[_threadNum];
        for(int i = 0; i < _threadNum; ++i)
            binWorkers[i] = new Worker(_conf, _destinationDir, _verify);

        for(Map.Entry<String, List<MyPath>> entry: _binLogFiles.entrySet()) {
            List<MyPath> bfs = entry.getValue();
            int bfNum = bfs.size();
            if(bfNum > 0) {
                int partitionSize = bfNum / _threadNum;
                int extraSize = bfNum % _threadNum;
                if(extraSize > 0) {
                    int startIdx = 0;
                    for (int i = 0; i < extraSize; ++i) {
                        binWorkers[i].assign(bfs.subList(startIdx, startIdx+(partitionSize+1)));
                        startIdx += partitionSize+1;
                    }

                    startIdx -= 1;
                    for (int j = extraSize; j < _threadNum; ++j) {
                        binWorkers[j].assign(bfs.subList(startIdx, startIdx+partitionSize));
                        startIdx += partitionSize;
                    }
                } else {
                    int startIdx = 0;
                    for(int i = 0; i < _threadNum; ++i) {
                        binWorkers[i].assign(bfs.subList(startIdx, startIdx+partitionSize));
                        startIdx += partitionSize;
                    }
                }
            }
        }

        for(int i = 0; i < _threadNum; ++i)
            binWorkers[i].start();

        Worker ackWorker = null;
        if(_ackLogFiles.size() > 0) {
            ackWorker = new Worker(_conf, _destinationDir, _verify);
            ackWorker.assign(_ackLogFiles);
            ackWorker.start();
        }

        /*
        List<Worker> binWorkers = new ArrayList<Worker>();
        int binLogFileNum = _binLogFiles.size();
        if(binLogFileNum > 0) {
            int partitionSize = binLogFileNum / _threadNum;
            int extraSize = binLogFileNum % _threadNum;
            if(extraSize > 0) {
                int startIdx = 0;
                for (int i = 0; i < extraSize; ++i) {
                    Worker worker = new Worker(_conf, _binLogFiles.subList(startIdx, startIdx+(partitionSize+1)),
					    _destinationDir, _verify);
                    worker.start();

                    binWorkers.add(worker);
                    startIdx += partitionSize+1;
                }

                startIdx -= 1;
                for (int j = extraSize; j < _threadNum; ++j) {
                    Worker worker = new Worker(_conf, _binLogFiles.subList(startIdx, startIdx+partitionSize),
					    _destinationDir, _verify);
                    worker.start();

                    binWorkers.add(worker);
                    startIdx += partitionSize;
                }
            } else {
                int startIdx = 0;
                for(int i = 0; i < _threadNum; ++i) {
                    Worker worker = new Worker(_conf,  _binLogFiles.subList(startIdx, startIdx+partitionSize),
						_destinationDir, _verify);
                    worker.start();

                    binWorkers.add(worker);
                    startIdx += partitionSize;
                }
            }
        }

        Worker ackWorker = null;
        if(_ackLogFiles.size() > 0) {
            ackWorker = new Worker(_conf, _ackLogFiles, _destinationDir, _verify);
            ackWorker.start();
        }
        */

        for (Worker binWorker : binWorkers)
            try {
                binWorker.join();
            } catch (InterruptedException ie) {
                // TODO: handle interrupted exception 'ie'
            }
        if(ackWorker != null)
            try {
                ackWorker.join();
            } catch (InterruptedException ie) {
                // TODO: handle interrupted exception 'ie'
            }
    }

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();

        DataIngester ingester = new DataIngester();
        ingester.loadConfig();

				long start = System.currentTimeMillis();
        ingester.ingest();
        System.out.println("Total time takes: " + (System.currentTimeMillis() - start) + " milliseconds.");
    }
}
