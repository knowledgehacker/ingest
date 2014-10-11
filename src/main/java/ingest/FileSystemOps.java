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

    private final boolean _verify;

    public FileSystemOps(int bufferSize, boolean verify) {
        _bufferSize = bufferSize;
        _buffer = new byte[bufferSize];

        _verify = verify;
    }

    public void copy(MyPath in, MyPath out) {
        long start = System.currentTimeMillis();

        FileSystem inFS = in.getFileSystem();
        Path inPath = in.getPath();
        InputStream is = null;
        FileSystem outFS = out.getFileSystem();
        Path outPath = out.getPath();
        OutputStream os = null;
        try {
            is = inFS.open(inPath, _bufferSize);
            if (outFS.exists(outPath))
                throw new RuntimeException("Can't copy file " + inPath.getName() + " to file " + outPath.getName() + " which already exists.");
            os = outFS.create(outPath);

            if (_verify)
                checkedWrite(is, outFS, outPath, os);
            else
                uncheckedWrite(is, os);
        } catch (IOException ioe) {
            // TODO: alert by e-mail
            throw new RuntimeException("Copy file " + inPath.getName() + " failed - " + ioe.toString());
        } finally {
            if(is != null)
                try {
                    is.close();
                }catch(IOException e) {

                }
            if(os != null)
                try {
                    os.close();
                }catch(IOException e) {

                }
        }
        System.out.println("Copy file " + inPath.getName() + " to file " + outPath.getName() + " takes " + (System.currentTimeMillis() - start) + " milliseconds.");
    }

    private final void checkedWrite(InputStream is, FileSystem outFS, Path outPath, OutputStream os) throws IOException {
        CRC32 inCrc32 = new CRC32();
        is = new CheckedInputStream(is, inCrc32);
        int bytesRead = -1;
        while ((bytesRead = is.read(_buffer, 0, _bufferSize)) > 0)
            os.write(_buffer, 0, bytesRead);

        CRC32 outCrc32 = new CRC32();
        is = new CheckedInputStream(outFS.open(outPath, _bufferSize), outCrc32);
        while (is.read(_buffer, 0, _bufferSize) > 0);
        if(inCrc32 != outCrc32)
            throw new RuntimeException("File " + outPath.getName() + " is corrupted during copying.");
    }

    private final void uncheckedWrite(InputStream is, OutputStream os) throws IOException {
        int bytesRead = -1;
        while ((bytesRead = is.read(_buffer, 0, _bufferSize)) > 0)
            os.write(_buffer, 0, bytesRead);
    }

    public final void move(MyPath from, MyPath to) throws IOException {
        if(!from.getScheme().equals(to.getScheme()))
            throw new RuntimeException("Files " + from.getPath() + " and " + to.getPath() + " have different schemes.");

        FileSystem fromFS = from.getFileSystem();
        FileSystem toFS = to.getFileSystem();
        fromFS.rename(from.getPath(), to.getPath());

        /*
        FileSystem fromFS = from.getFileSystem();
        Path fromPath = from.getPath();
        InputStream is = fromFS.open(fromPath, _bufferSize);
        FileSystem toFS = to.getFileSystem();
        Path toPath = to.getPath();
        if (toFS.exists(toPath))
            throw new RuntimeException("Can't copy file " + fromPath.getName() + " to file " + toPath.getName() + " which already exists.");
        OutputStream os = fromFS.create(fromPath);
        uncheckedWrite(is, os);
        */
    }

    public final void delete(MyPath path) throws IOException {
        path.getFileSystem().delete(path.getPath(), false);
    }
}
