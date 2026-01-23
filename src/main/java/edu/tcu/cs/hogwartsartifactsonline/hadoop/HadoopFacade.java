package hadoop;

import edu.tcu.cs.hogwartsartifactsonline.hadoop.HBaseService;
import edu.tcu.cs.hogwartsartifactsonline.hadoop.HdfsService;
import jakarta.annotation.PostConstruct;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration as SpringConfig;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

@SpringConfig
public class HadoopFacade {

    @Value("${hadoop.principal}")
    private String principal;

    @Value("${hadoop.keytab}")
    private String keytab;

    @Value("${hadoop.fsDefault}")
    private String fsDefault;

    @Value("${hive.jdbc.url}")
    private String hiveJdbcUrl;

    @Value("${hive.principal}")
    private String hivePrincipal;

    @Value("${impala.jdbc.url}")
    private String impalaJdbcUrl;

    private Configuration hadoopConf;
    private org.apache.hadoop.conf.Configuration hbaseConf;

    @PostConstruct
    public void init() throws IOException {
        hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", fsDefault);
        hadoopConf.set("hadoop.security.authentication", "kerberos");

        UserGroupInformation.setConfiguration(hadoopConf);
        UserGroupInformation.loginUserFromKeytab(principal, keytab);

        hbaseConf = HBaseConfiguration.create(hadoopConf);
        hbaseConf.set("hbase.security.authentication", "kerberos");
    }

    <T> T run(PrivilegedExceptionAction<T> action) {
        try {
            return UserGroupInformation.getLoginUser().doAs(action);
        } catch (Exception e) {
            throw new RuntimeException("Kerberos execution failed", e);
        }
    }

    public HdfsService hdfs() {
        return new HdfsService(this, hadoopConf);
    }

    public HBaseService hbase() {
        return new HBaseService(this, hbaseConf);
    }

    public hadoop.jdbc.HiveService hive() {
        return new hadoop.jdbc.HiveService(this, hiveJdbcUrl, hivePrincipal);
    }

    public hadoop.jdbc.ImpalaService impala() {
        return new hadoop.jdbc.ImpalaService(this, impalaJdbcUrl);
    }
}