package ingest;

/**
 * Created by mlin on 10/9/14.
 */
import java.util.Properties;
import java.util.List;
import java.util.ArrayList;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class DataIngester {
    private final Logger LOG = LoggerFactory.getLogger(DataIngester.class);

    private final String CONFIG_FILE = "/config.properties";

    private final String DATA_DATE_START = "data.date.start";
    private final String DATA_DATE_END = "data.date.end";
    private final String DATA_SOURCE_DIR = "data.source.dir";
    private final String DATA_WORK_DIR = "data.work.dir";
    private final String DATA_DESTINATION_DIR = "data.destination.dir";
    private final String VERIFY = "verify";
    private final String THREAD_NUM = "thread.num";

    private String _dateStart;
    private String _dateEnd;
    private String _sourceDir;
    private String _workDir;
    private String _destinationDir;
    private boolean _verify;
    private int _threadNum;

    private Configuration _conf;

    private FileSystemOps _ops;
    private List<MyPath> _logFiles;

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
        _workDir = properties.getProperty(DATA_WORK_DIR);
        _destinationDir = properties.getProperty(DATA_DESTINATION_DIR);
        _verify = Boolean.parseBoolean(properties.getProperty(VERIFY));
        _threadNum = Integer.parseInt(properties.getProperty(THREAD_NUM));

        _conf = new Configuration();

        _ops = new FileSystemOps(4096, _verify);
        _logFiles = new ArrayList<MyPath>();
    }

    public void ingest() throws IOException {
        // copy files in _sourceDir/[data_center]/binary/[ack|bin]-server_id-timestamp.log.gz to _destinationDir/[ack|bin]-server_id-timestamp.log.gz
        File dataSourceDir = null;
		try {
			dataSourceDir = new File(new URI(_sourceDir));
		} catch(URISyntaxException urise) {
			throw new RuntimeException("Open directory " + dataSourceDir + " failed - " + urise.toString());
        }

        File[] dataCenterDirs = dataSourceDir.listFiles();
        for(File datacenterDir: dataCenterDirs) {
            File binaryDir = new File(datacenterDir, "binary");
            File[] logFiles = binaryDir.listFiles();
            for(File logFile: logFiles) {
				String logFileName = logFile.getName();
                if(!(logFileName.startsWith("ack") || logFileName.startsWith("bin")))
                    continue;
                String[] parts = logFileName.substring(0, logFileName.indexOf('.')).split("-");
                String timestamp = parts[parts.length-1];
                if(timestamp.compareTo(_dateStart) >= 0 && timestamp.compareTo(_dateEnd) <= 0) {
                    MyPath sourceFile = new MyPath(_conf, new Path(_sourceDir + "/" + datacenterDir.getName() + "/" + binaryDir.getName() + "/" + logFile.getName()));
                    MyPath workFile = new MyPath(_conf, new Path(_workDir + "/" + logFile.getName()));
                    _ops.move(sourceFile, workFile);

                    _logFiles.add(workFile);
                }
            }
        }

        int partitionSize = _logFiles.size() / _threadNum;
        int extraSize = _logFiles.size() % _threadNum;
        for(int i = 0; i < extraSize; ++i)
            new Worker(_conf, _ops, _logFiles.subList(i*(partitionSize+1), (i+1)*(partitionSize+1)), _workDir, _destinationDir).start();

        int startIdx = extraSize * (partitionSize+1);
        for(int j = extraSize; j < _threadNum; ++j)
            new Worker(_conf, _ops, _logFiles.subList(j*partitionSize+startIdx, (j+1)*partitionSize+startIdx), _workDir, _destinationDir).start();
    }

    public static void main(String[] args) throws IOException {
        DataIngester ingester = new DataIngester();
        ingester.loadConfig();

        ingester.ingest();
    }
}
