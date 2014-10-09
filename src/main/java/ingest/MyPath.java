package ingest;

/**
 * Created by mlin on 10/9/14.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class MyPath {
    private final FileSystem _fs;
    private final Path _path;
    private final String _scheme;

    public MyPath(Configuration conf, Path path) throws IOException {
        _path = path;
        _scheme = _path.toUri().getScheme();
        System.out.println("_scheme: " + _scheme);
        _fs = path.getFileSystem(conf);
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
