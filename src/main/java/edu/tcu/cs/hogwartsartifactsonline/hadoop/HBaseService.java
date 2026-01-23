package hadoop;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseService {

    private final HadoopFacade facade;
    private final org.apache.hadoop.conf.Configuration conf;

    HBaseService(HadoopFacade facade, org.apache.hadoop.conf.Configuration conf) {
        this.facade = facade;
        this.conf = conf;
    }

    public Result get(String table, String rowKey) {
        return facade.run(() -> {
            try (Connection conn = ConnectionFactory.createConnection(conf);
                 Table t = conn.getTable(TableName.valueOf(table))) {

                return t.get(new Get(Bytes.toBytes(rowKey)));
            }
        });
    }

    public void put(String table, Put put) {
        facade.run(() -> {
            try (Connection conn = ConnectionFactory.createConnection(conf);
                 Table t = conn.getTable(TableName.valueOf(table))) {

                t.put(put);
            }
            return null;
        });
    }
}