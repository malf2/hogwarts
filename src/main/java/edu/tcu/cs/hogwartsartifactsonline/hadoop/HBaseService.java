package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;

public class HBaseService {

    private final HadoopFacade facade;
    private final Configuration conf;

    HBaseService(HadoopFacade facade, Configuration conf) {
        this.facade = facade;
        this.conf = conf;
    }

    /* =========================
       Basic data operations
       ========================= */

    public Result get(String table, Get get) {
        return facade.run(() -> {
            try (Connection conn = ConnectionFactory.createConnection(conf);
                 Table t = conn.getTable(TableName.valueOf(table))) {

                return t.get(get);
            }
        });
    }

    public Result get(String table, String rowKey) {
        return get(table, new Get(Bytes.toBytes(rowKey)));
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

    public void put(String table, List<Put> puts) {
        facade.run(() -> {
            try (Connection conn = ConnectionFactory.createConnection(conf);
                 Table t = conn.getTable(TableName.valueOf(table))) {

                t.put(puts);
            }
            return null;
        });
    }

    public void delete(String table, Delete delete) {
        facade.run(() -> {
            try (Connection conn = ConnectionFactory.createConnection(conf);
                 Table t = conn.getTable(TableName.valueOf(table))) {

                t.delete(delete);
            }
            return null;
        });
    }

    /* =========================
       Scan operations
       ========================= */

    public List<Result> scan(String table, Scan scan) {
        return facade.run(() -> {
            try (Connection conn = ConnectionFactory.createConnection(conf);
                 Table t = conn.getTable(TableName.valueOf(table));
                 ResultScanner scanner = t.getScanner(scan)) {

                List<Result> results = new ArrayList<>();
                for (Result r : scanner) {
                    results.add(r);
                }
                return results;
            }
        });
    }

    public List<Result> scan(String table) {
        return scan(table, new Scan());
    }

    /* =========================
       Table admin operations
       ========================= */

    public boolean tableExists(String table) {
        return facade.run(() -> {
            try (Connection conn = ConnectionFactory.createConnection(conf);
                 Admin admin = conn.getAdmin()) {

                return admin.tableExists(TableName.valueOf(table));
            }
        });
    }

    public void createTable(TableDescriptor tableDescriptor) {
        facade.run(() -> {
            try (Connection conn = ConnectionFactory.createConnection(conf);
                 Admin admin = conn.getAdmin()) {

                admin.createTable(tableDescriptor);
            }
            return null;
        });
    }

    public void deleteTable(String table) {
        facade.run(() -> {
            try (Connection conn = ConnectionFactory.createConnection(conf);
                 Admin admin = conn.getAdmin()) {

                TableName tn = TableName.valueOf(table);
                if (admin.isTableEnabled(tn)) {
                    admin.disableTable(tn);
                }
                admin.deleteTable(tn);
            }
            return null;
        });
    }
}