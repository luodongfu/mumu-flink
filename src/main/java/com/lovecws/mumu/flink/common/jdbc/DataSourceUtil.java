package com.lovecws.mumu.flink.common.jdbc;

import com.lovecws.mumu.flink.common.config.ConfigProperties;

import java.io.Serializable;
import java.util.Map;

/**
 * @program: act-industry-data-common
 * @description: 数据库工具
 * @author: 甘亮
 * @create: 2019-11-07 16:27
 **/
public class DataSourceUtil implements Serializable {

    private static JdbcConfig jdbcConfig;
    private static final Object object = new Object();

    public static JdbcConfig getJdbcConfig() {
        if (jdbcConfig == null) {
            synchronized (object) {
                if (jdbcConfig == null) {
                    Map<String, Object> dataSourceConfig = ConfigProperties.getMap("spring.datasource.default");
                    String url = dataSourceConfig.getOrDefault("url", "").toString();
                    String driver = dataSourceConfig.getOrDefault("driver-class-name", "").toString();
                    String user = dataSourceConfig.getOrDefault("username", "").toString();
                    String password = dataSourceConfig.getOrDefault("password", "").toString();
                    String charset = dataSourceConfig.getOrDefault("charset", "").toString();
                    jdbcConfig = new JdbcConfig(url, driver, user, password, charset);
                }
            }
        }
        return jdbcConfig;
    }
}
