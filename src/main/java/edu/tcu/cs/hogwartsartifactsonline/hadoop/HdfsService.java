package hadoop;

import org.apache.hadoop.fs.*;

public class HdfsService {

    private final HadoopFacade facade;
    private final org.apache.hadoop.conf.Configuration conf;

    HdfsService(HadoopFacade facade, org.apache.hadoop.conf.Configuration conf) {
        this.facade = facade;
        this.conf = conf;
    }

    public boolean exists(String path) {
        return facade.run(() -> {
            try (FileSystem fs = FileSystem.get(conf)) {
                return fs.exists(new Path(path));
            }
        });
    }

    public FileStatus[] listStatus(String path) {
        return facade.run(() -> {
            try (FileSystem fs = FileSystem.get(conf)) {
                return fs.listStatus(new Path(path));
            }
        });
    }

    public boolean mkdirs(String path) {
        return facade.run(() -> {
            try (FileSystem fs = FileSystem.get(conf)) {
                return fs.mkdirs(new Path(path));
            }
        });
    }

    public boolean delete(String path, boolean recursive) {
        return facade.run(() -> {
            try (FileSystem fs = FileSystem.get(conf)) {
                return fs.delete(new Path(path), recursive);
            }
        });
    }
}