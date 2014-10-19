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
    private final String BIN_ACK_RATIO = "bin.ack.ratio";

    private String _dateStart;
    private String _dateEnd;
    private String _sourceDir;
    private String _destinationDir;
    private boolean _verify;
    private int _binThreadNum;
    private int _ackThreadNum;

    private Configuration _conf;

    private Map<String, List<MyPath>> _binLogFiles;
    private Map<String, List<MyPath>> _ackLogFiles;

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
        int ratio = Integer.parseInt(properties.getProperty(BIN_ACK_RATIO));
        int processorNum = Runtime.getRuntime().availableProcessors() * 1 / 2;
        _binThreadNum = ratio * processorNum / (ratio+1);
        _ackThreadNum = processorNum / (ratio+1);

        _conf = new Configuration();

        _binLogFiles = new HashMap<String, List<MyPath>>();
        _ackLogFiles = new HashMap<String, List<MyPath>>();
    }

    public void ingest() throws IOException {
        File dataSourceDir = null;
        try {
            dataSourceDir = new File(new URI(_sourceDir));
        } catch (URISyntaxException urise) {
            throw new RuntimeException("Open directory " + dataSourceDir + " failed - " + urise.toString());
        }

        File[] servers = dataSourceDir.listFiles();
        for(File server: servers) {
            File binaryDir = new File(server, "binary");
            for(File logFile: binaryDir.listFiles()) {
                String logFileName = logFile.getName();
                if(!(logFileName.startsWith("ack") || logFileName.startsWith("bin")))
                    continue;
                String timestamp = logFileName.substring(logFileName.lastIndexOf('-')+1, logFileName.indexOf('.'));
                if(timestamp.compareTo(_dateStart) >= 0 && timestamp.compareTo(_dateEnd) <= 0) {
                    String serverName = server.getName();
                    MyPath sourceFile = new MyPath(_conf, new Path(_sourceDir + "/" + serverName + "/" + "binary" + "/" + logFileName));
                    if(logFileName.startsWith("bin"))
                        classify(serverName, sourceFile, _binLogFiles);
                   else
                        classify(serverName, sourceFile, _ackLogFiles);
                }
            }
        }

        // copy bin log files
        Worker[] binWorkers = dispatch(_binLogFiles, _binThreadNum);
        // copy ack log files
        Worker[] ackWorkers = dispatch(_ackLogFiles, _ackThreadNum);

        // wait for bin workers to complete
        waitForWorkers(binWorkers);
        // wait for ack workers to complete
        waitForWorkers(ackWorkers);
    }

    private final void classify(String serverName, MyPath sourceFile, Map<String, List<MyPath>> dcWorkload) {
        String dataCenterName = serverName.substring(0, serverName.indexOf("ads"));
        List<MyPath> sourceFiles = dcWorkload.get(dataCenterName);
        if(sourceFiles == null)
            sourceFiles = new ArrayList<MyPath>();
        sourceFiles.add(sourceFile);
        dcWorkload.put(dataCenterName, sourceFiles);
    }

    private final Worker[] dispatch(Map<String, List<MyPath>> dcWorkload, int threadNum) {
        List<MyPath>[] workLoads = (List<MyPath>[]) new ArrayList[threadNum];
        for(int i = 0; i < threadNum; ++i)
            workLoads[i] = new ArrayList<MyPath>();

        int extraIdx = 0;
        for (Map.Entry<String, List<MyPath>> entry : dcWorkload.entrySet()) {
            List<MyPath> bfs = entry.getValue();
            int bfNum = bfs.size();
            if (bfNum > 0) {
                int partitionSize = bfNum / threadNum;
                int extraSize = bfNum % threadNum;
                int startIdx = 0;
                for (int i = 0; i < threadNum; ++i) {
                    workLoads[i].addAll(bfs.subList(startIdx, startIdx + partitionSize));
                    startIdx += partitionSize;
                }
                if (extraSize > 0)
                    for (int j = 0; j < extraSize; ++j)
                        workLoads[extraIdx++ % threadNum].add(bfs.get(startIdx + j));
            }
        }

        Worker[] workers = new Worker[threadNum];
        for (int i = 0; i < threadNum; ++i) {
            workers[i] = new Worker(_conf, workLoads[i], _destinationDir, _verify);
            workers[i].start();
        }

        return workers;
    }

    private final void waitForWorkers(Worker[] workers) {
        for (Worker worker : workers)
            try {
                worker.join();
            } catch (InterruptedException ie) {
                // TODO: handle interrupted exception 'ie'
            }
    }

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();

        DataIngester ingester = new DataIngester();
        ingester.loadConfig();

        ingester.ingest();
        System.out.println("Total time takes: " + (System.currentTimeMillis() - start) + " milliseconds.");
    }
}
