package hadoop.jdbc;

import hadoop.HadoopFacade;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public abstract class JdbcKerberosService {

    protected final HadoopFacade facade;

    protected JdbcKerberosService(HadoopFacade facade) {
        this.facade = facade;
    }

    protected abstract Connection getConnection() throws Exception;

    public List<List<Object>> query(String sql, int timeoutSeconds) {
        return facade.run(() -> {
            try (Connection conn = getConnection();
                 Statement st = conn.createStatement()) {

                st.setQueryTimeout(timeoutSeconds);

                try (ResultSet rs = st.executeQuery(sql)) {
                    List<List<Object>> rows = new ArrayList<>();
                    int cols = rs.getMetaData().getColumnCount();

                    while (rs.next()) {
                        List<Object> row = new ArrayList<>();
                        for (int i = 1; i <= cols; i++) {
                            row.add(rs.getObject(i));
                        }
                        rows.add(row);
                    }
                    return rows;
                }
            }
        });
    }

    public void execute(String sql, int timeoutSeconds) {
        facade.run(() -> {
            try (Connection conn = getConnection();
                 Statement st = conn.createStatement()) {

                st.setQueryTimeout(timeoutSeconds);
                st.execute(sql);
            }
            return null;
        });
    }

    public int update(String sql, int timeoutSeconds) {
        return facade.run(() -> {
            try (Connection conn = getConnection();
                 Statement st = conn.createStatement()) {

                st.setQueryTimeout(timeoutSeconds);
                return st.executeUpdate(sql);
            }
        });
    }

    public List<List<Object>> query(String sql) {
        return query(sql, 0);
    }

    public void execute(String sql) {
        execute(sql, 0);
    }

    public int update(String sql) {
        return update(sql, 0);
    }
}