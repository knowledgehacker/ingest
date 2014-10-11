package ingest;

/**
 * Created by mlin on 10/9/14.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.io.IOException;

public class MyPath {
    private final String _scheme;
    private final Path _path;
    private final FileSystem _fs;

    public MyPath(Configuration conf, Path path) throws IOException {
        URI uri = path.toUri();
        _scheme = uri.getScheme();
        if(_scheme.equals("hdfs")) {
            String prefix = _scheme + "://";
            String pathPart = path.toString().substring(prefix.length());

            String defaultFS = conf.get("fs.defaultFS");
            String absolutePath = prefix + defaultFS.substring(prefix.length()) + "/" + pathPart;
            _path = new Path(absolutePath);
        } else
            _path = path;

        _fs = _path.getFileSystem(conf);
    }

    public final FileSystem getFileSystem() {
        return _fs;
    }

    public final String getScheme() {
        return _scheme;
    }

    public final Path getPath() {
        return _path;
    }
}
