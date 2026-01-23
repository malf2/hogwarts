package hadoop.jdbc;

import hadoop.HadoopFacade;

import java.sql.Connection;
import java.sql.DriverManager;

public class ImpalaService extends hadoop.jdbc.JdbcKerberosService {

    private final String jdbcUrl;

    public ImpalaService(HadoopFacade facade, String jdbcUrl) {
        super(facade);
        this.jdbcUrl = jdbcUrl;
    }

    @Override
    protected Connection getConnection() throws Exception {
        return DriverManager.getConnection(jdbcUrl);
    }
}