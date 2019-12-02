package com.lovecws.mumu.flink.common.jdbc;


import com.alibaba.druid.pool.DruidDataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * jdbc 数据源配置信息
 */
public class JdbcConfig implements Serializable {

    private static final Map<JdbcConfig, DruidDataSource> dataSourceMap = new ConcurrentHashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(JdbcConfig.class);
    private static final String DEFAULT_CHARSET = "ISO-8859-1";

    private String jdbcType;
    private String database;

    private Date currentDate;
    private Map<String, String> tableInfo;

    private String url;
    private String driver;
    private String user;
    private String password;
    private String charset;//数据库编码

    public JdbcConfig(String url, String driver, String user, String password, String charset, Properties properties) {
        if (StringUtils.isEmpty(charset)) charset = DEFAULT_CHARSET;
        this.url = url;
        this.driver = driver;
        this.user = user;
        this.password = password;
        this.charset = charset;
        getDruidDataSource();
    }

    public JdbcConfig(String url, String driver, String user, String password, String charset) {
        this(url, driver, user, password, charset, null);
    }

    public JdbcConfig(String url, String driver, String user, String password) {
        this(url, driver, user, password, DEFAULT_CHARSET);
    }

    /**
     * 获取druid连接池
     *
     * @return
     */
    private DruidDataSource getDruidDataSource() {
        DruidDataSource dataSource = dataSourceMap.get(this);
        if (dataSource == null) {
            DruidDataSource druidDataSource = new DruidDataSource();
            druidDataSource.setUrl(url);
            setJdbcUrl(url);
            druidDataSource.setDriverClassName(driver);
            druidDataSource.setUsername(user);
            druidDataSource.setPassword(password);

            druidDataSource.setTestOnBorrow(true);
            druidDataSource.setTestOnReturn(false);
            druidDataSource.setTestWhileIdle(true);
            druidDataSource.setValidationQuery("select 1");

            druidDataSource.setMaxActive(20);
            druidDataSource.setInitialSize(1);
            druidDataSource.setMinIdle(1);

            String version = druidDataSource.getVersion();
            try {
                druidDataSource.init();
            } catch (Throwable ex) {
                druidDataSource.close();
                String localizedMessage = ex.getLocalizedMessage();
                if (localizedMessage == null) localizedMessage = "";
                //数据库未创建，使用该数据库连接会报错
                if (localizedMessage.contains("Database") && localizedMessage.contains("does not exist")) {
                    if ("hive".equalsIgnoreCase(jdbcType) || "hive2".equalsIgnoreCase(jdbcType)) {
                        DruidDataSource ds = new DruidDataSource();
                        ds.setDriverClassName(driver);
                        ds.setUsername(user);
                        ds.setPassword(password);
                        ds.setUrl(url.replace(database, "default"));
                        try {
                            //初始化默认数据库连接
                            ds.init();
                            dataSourceMap.put(this, ds);
                            AbstractJdbcService jdbcService = new HiveJdbcService(this);
                            jdbcService.createDatabase(database);
                            ds.close();
                            dataSourceMap.remove(this);
                            //使用新创建的数据库连接
                            return getDruidDataSource();
                        } catch (Exception e) {
                            logger.error(e.getLocalizedMessage());
                        }
                    }
                }
            }

            logger.info(version);
            dataSourceMap.put(this, druidDataSource);

            logger.info("DataSource " + database + " Inject Successfully...");

            return druidDataSource;
        }
        return dataSource;
    }

    /**
     * 获取数据库连接
     *
     * @return
     */
    public Connection getConnection() {
        try {
            DruidDataSource dataSource = dataSourceMap.get(this);
            if (dataSource == null) {
                dataSource = getDruidDataSource();
            }
            return dataSource.getConnection();
        } catch (Exception ex) {
            logger.error(ex.getLocalizedMessage(), ex);
        }
        return null;
    }

    public void setJdbcUrl(String url) {
        if (url == null) {
            return;
        }
        String[] split = url.split(":");
        if (split.length >= 2) {
            jdbcType = split[1];
        }

        //获取数据库名称
        int i = url.lastIndexOf("/");
        if (i > -1) database = url.substring(i + 1);
    }

    public String getJdbcType() {
        return jdbcType == null ? "" : jdbcType;
    }

    public String getDatabase() {
        return database;
    }

    public String getSchema() {
        //postgresql 的schema为public
        if ("postgresql".equalsIgnoreCase(jdbcType)) {
            return null;
        }
        return database;
    }

    public String getCharset() {
        return charset;
    }

    public Date getCurrentDate() {
        return currentDate;
    }

    public void setCurrentDate(Date currentDate) {
        this.currentDate = currentDate;
    }

    public Map<String, String> getTableInfo() {
        return tableInfo;
    }

    public void setTableInfo(Map<String, String> tableInfo) {
        this.tableInfo = tableInfo;
    }

    public javax.sql.DataSource getDataSource() {
        return dataSourceMap.get(this);
    }
}