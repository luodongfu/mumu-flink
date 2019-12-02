/**
 * Project Name:stream-analyse
 * File Name:StormProUtils.java
 * Date:2016年3月8日下午5:26:32
 */

package com.lovecws.mumu.flink.common.util;

import java.io.Serializable;
import java.util.Properties;

/**
 * ClassName:StormProUtils <br/>
 * Date:     2016年3月8日 下午5:26:32 <br/>
 *
 * @author quanli
 * @see
 * @since JDK 1.6
 */
public class PropertiesUtils implements Serializable {

    //文件名称
    private static final String CONFIG_FILE = "config.properties";

    private Properties pro;

    public PropertiesUtils() {
    }

    /**
     * newInstance:构建默认对象. <br/>
     *
     * @return
     * @author quanli
     * @since JDK 1.6
     */
    public synchronized PropertiesUtils newInstance() {
        try {
            if (pro == null) {
                pro = new Properties();
                pro.load(ClassLoader.getSystemResourceAsStream(CONFIG_FILE));
                pro.list(System.out);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return this;
    }

    public synchronized PropertiesUtils newInstance(Properties properties) {
        if (pro == null) {
            pro = properties;
            pro.list(System.out);
        }
        return this;
    }

    /**
     * getParam:获取配置文件中的参数. <br/>
     *
     * @param key        主键
     * @param defaultVal 默认值
     * @return
     * @author quanli
     * @since JDK 1.6
     */
    public String getParam(String key, String defaultVal) {
        String param = pro.getProperty(key);
        if (param == null || "".equals(param)) {
            return defaultVal;
        } else {
            return param;
        }
    }

    /**
     * getParam:获取配置文件中的参数. <br/>
     *
     * @param key        主键
     * @param defaultVal 默认值
     * @return
     * @author ganliang
     * @since JDK 1.6
     */
    public Integer getInteger(String key, int defaultVal) {
        String property = pro.getProperty(key);
        if (property == null || "".equals(property)) {
            property = String.valueOf(defaultVal);
        }
        return Integer.parseInt(property);
    }

    /**
     * @param key        主键
     * @param defaultVal 默认值
     * @return
     * @author ganliang
     * @since JDK 1.6
     */
    public Boolean getBoolean(String key, boolean defaultVal) {
        String property = pro.getProperty(key);
        if (property == null || "".equals(property)) {
            property = String.valueOf(defaultVal);
        }
        return Boolean.parseBoolean(property);
    }

    public Long getLong(String key, long defaultVal) {
        String property = pro.getProperty(key);
        if (property == null || "".equals(property)) {
            property = String.valueOf(defaultVal);
        }
        return Long.parseLong(property);
    }
}

