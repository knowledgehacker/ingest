package ingest;

/**
 * Created by mlin on 10/9/14.
 */
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileSystemOps {
    private final int _bufferSize;
    private final byte[] _buffer;

    private final boolean _needsVerify;

    public FileSystemOps(int bufferSize, boolean needsVerify) {
        _bufferSize = bufferSize;
        _buffer = new byte[bufferSize];

        _needsVerify = needsVerify;
    }

    public void copy(MyPath in, MyPath out) throws IOException {
        FileSystem inFS = in.getFileSystem();
        Path inPath = in.getPath();
        InputStream is = inFS.open(inPath, _bufferSize);

        FileSystem outFS = out.getFileSystem();
        Path outPath = out.getPath();
        if(outFS.exists(outPath))
            throw new RuntimeException("Can't copy file " + inPath.getName() + " to file " + outPath.getName() + " which already exists.");
        OutputStream os = outFS.create(outPath);

        int bytesRead = -1;
        if(_needsVerify) {
            CRC32 inCrc32 = new CRC32();
            CheckedInputStream checkedInputStream = new CheckedInputStream(is, inCrc32);

            CRC32 outCrc32 = new CRC32();
            CheckedOutputStream checkedOutputStream = new CheckedOutputStream(os, outCrc32);

            while ((bytesRead = checkedInputStream.read(_buffer, 0, _bufferSize)) == _bufferSize)
                checkedOutputStream.write(_buffer, 0, bytesRead);

            if(inCrc32 != outCrc32)
                throw new RuntimeException("File " + outPath.getName() + " is corrupted during copying.");
        } else {
            while ((bytesRead = is.read(_buffer, 0, _bufferSize)) == _bufferSize)
                os.write(_buffer, 0, bytesRead);
        }
    }

    public final void move(MyPath from, MyPath to) throws IOException {
        if(from.getScheme().equals(to.getScheme()))
            throw new RuntimeException("Files " + from.getPath() + " and " + to.getPath() + " have different schemes.");

        FileSystem fromFS = from.getFileSystem();
        FileSystem toFS = to.getFileSystem();
        fromFS.rename(from.getPath(), to.getPath());
    }

    public final void delete(MyPath path) throws IOException {
        path.getFileSystem().delete(path.getPath(), false);
    }
}
