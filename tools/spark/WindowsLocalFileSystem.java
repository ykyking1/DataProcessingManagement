package project.spark;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

/** Local Windows filesystem adapter that does not require winutils.exe. */
public final class WindowsLocalFileSystem extends RawLocalFileSystem {
    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        File file = pathToFile(path);
        if (!file.exists()) {
            throw new FileNotFoundException("File does not exist: " + path);
        }
        Path qualifiedPath = makeQualified(path);
        return new FileStatus(
            file.length(),
            file.isDirectory(),
            1,
            getDefaultBlockSize(qualifiedPath),
            file.lastModified(),
            qualifiedPath
        );
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        File file = pathToFile(path);
        if (!file.exists()) {
            throw new FileNotFoundException("File does not exist: " + path);
        }
        if (file.isFile()) {
            return new FileStatus[] {getFileStatus(path)};
        }

        File[] children = file.listFiles();
        if (children == null) {
            throw new IOException("Cannot list local directory: " + path);
        }
        FileStatus[] statuses = new FileStatus[children.length];
        for (int index = 0; index < children.length; index++) {
            statuses[index] = getFileStatus(new Path(path, children[index].getName()));
        }
        return statuses;
    }

    @Override
    public void setPermission(Path path, FsPermission permission) throws IOException {
        // Windows ACLs are managed by the operating system.
    }
}
