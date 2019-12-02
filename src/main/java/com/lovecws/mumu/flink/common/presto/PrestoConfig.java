package com.lovecws.mumu.flink.common.presto;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

/**
 * @program: act-able
 * @description: 云企业presto客户端
 * @author: 甘亮
 * @create: 2019-02-20 17:23
 **/
public class PrestoConfig {

    private static final Logger logger = LoggerFactory.getLogger(PrestoConfig.class);

    private static DataSource dataSource;

    private String host;

    private int port;

    private String username;

    private String password;

    public PrestoConfig(String host, int port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    /**
     * 没法做成database实体，和druid冲突
     *
     * @return
     */
    private synchronized DataSource dataSource() {
        if (dataSource == null) {
            dataSource = new DataSource();
            dataSource.setUrl("jdbc:presto://" + host + ":" + port + "/hive");
            dataSource.setDriverClassName("com.facebook.presto.jdbc.PrestoDriver");
            dataSource.setUsername(username);
            dataSource.setPassword(password);
            logger.debug("Presto DataSource Inject Successfully...");
        }
        return dataSource;
    }

    public Connection getConnection() {
        try {
            return dataSource().getConnection();
        } catch (Exception ex) {
            logger.error(ex.getLocalizedMessage(), ex);
        }
        return null;
    }

   /* public static void main(String[] args) {
        PrestoConfig prestoConfig = new PrestoConfig("", 9001, "", "");
        Connection connection = prestoConfig.getConnection();
        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select *from isdms.t_dm_enterprise_city limit 10");
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(resultSet.getObject(i) + "  ");
                }
                System.out.println();
            }
            resultSet.close();
            statement.cancel();
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }*/
}
