package hadoop.jdbc;

import hadoop.HadoopFacade;

import java.sql.Connection;
import java.sql.DriverManager;

public class HiveService extends hadoop.jdbc.JdbcKerberosService {

    private final String jdbcUrl;
    private final String principal;

    public HiveService(HadoopFacade facade, String jdbcUrl, String principal) {
        super(facade);
        this.jdbcUrl = jdbcUrl;
        this.principal = principal;
    }

    @Override
    protected Connection getConnection() throws Exception {
        return DriverManager.getConnection(
                jdbcUrl + ";principal=" + principal
        );
    }
}