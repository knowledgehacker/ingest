package ingest;

/**
 * Created by mlin on 10/9/14.
 */
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileSystemOps {
    private final int _bufferSize;
    private final byte[] _buffer;

    public FileSystemOps(int bufferSize) {
        _bufferSize = bufferSize;
        _buffer = new byte[bufferSize];
    }

    public void copy(MyPath in, MyPath out, boolean verify) throws IOException {
        long start = System.currentTimeMillis();

        InputStream is = null;
        OutputStream os = null;
        try {
            FileSystem inFS = in.getFileSystem();
            Path inPath = in.getPath();
            is = inFS.open(inPath, _bufferSize);
            FileSystem outFS = out.getFileSystem();
            Path outPath = out.getPath();
            if (outFS.exists(outPath))
                throw new RuntimeException("File " + outPath + " already exists.");

            os = outFS.create(outPath);

            if (verify)
                checkedWrite(is, os, outFS, outPath);
            else
                uncheckedWrite(is, os);
        } finally {
            if(is != null)
                is.close();
            if(os != null)
                os.close();
        }
        //System.out.println("Copy file " + in.getPath() + " to " + out.getPath() + " takes " + (System.currentTimeMillis() - start) + " milliseconds.");
    }

    private final void checkedWrite(InputStream is, OutputStream os, FileSystem outFS, Path outPath) throws IOException {
        CRC32 inCrc32 = new CRC32();
        is = new CheckedInputStream(is, inCrc32);
        int bytesRead = -1;
        while ((bytesRead = is.read(_buffer, 0, _bufferSize)) > 0)
            os.write(_buffer, 0, bytesRead);

        CRC32 outCrc32 = new CRC32();
        is = new CheckedInputStream(outFS.open(outPath, _bufferSize), outCrc32);
        while (is.read(_buffer, 0, _bufferSize) > 0);
        if(inCrc32 != outCrc32)
            throw new RuntimeException("File " + outPath + " is corrupted during copying.");
    }

    private final void uncheckedWrite(InputStream is, OutputStream os) throws IOException {
        int bytesRead = -1;
        while ((bytesRead = is.read(_buffer, 0, _bufferSize)) > 0)
            os.write(_buffer, 0, bytesRead);
    }

    public final boolean exists(MyPath path) throws IOException {
        return path.getFileSystem().exists(path.getPath());
    }

    public final void delete(MyPath path) throws IOException {
        path.getFileSystem().delete(path.getPath(), false);
    }
}
