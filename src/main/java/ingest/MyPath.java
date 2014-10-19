package ingest;

/**
 * Created by mlin on 10/9/14.
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MyPath {
    private final String _scheme;
    private final Path _path;
    private final FileSystem _fs;

    public MyPath(Configuration conf, Path path) throws IOException {
        _scheme = path.toUri().getScheme();
        if(_scheme.equals("hdfs"))
            _path = new Path(conf.get("fs.defaultFS") + "/" + path.toString().substring((_scheme + "://").length()));
        else
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
