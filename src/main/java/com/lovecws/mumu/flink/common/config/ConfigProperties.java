package com.lovecws.mumu.flink.common.config;

import com.lovecws.mumu.flink.common.util.MapFieldUtil;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @program: act-industry-data-common
 * @description: 配置
 * @author: 甘亮
 * @create: 2019-11-07 14:46
 **/
public class ConfigProperties implements Serializable {

    private static final String bootstrap = "/bootstrap.yml";
    private static final String bootstrap_config = "/config";
    private static Map<String, Object> PROFILE_PROPERTIES = null;

    public static Map<String, Object> loadConfig() {
        if (PROFILE_PROPERTIES == null) {
            try {
                //配置文件路径
                InputStream reader = ConfigProperties.class.getResourceAsStream(bootstrap);

                //yml读取配置文件,指定返回类型为Map,Map中value类型为LinkedHashMap
                Map map = new Yaml().loadAs(reader, LinkedHashMap.class);
                Object activeProfile = MapFieldUtil.getMapField(map, "spring.profiles.active", "");
                if (activeProfile == null) throw new IllegalArgumentException();

                String profile = bootstrap_config + "/bootstrap-" + activeProfile + ".yml";
                InputStream readerDev = ConfigProperties.class.getResourceAsStream(profile);
                PROFILE_PROPERTIES = new Yaml().loadAs(readerDev, LinkedHashMap.class);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return PROFILE_PROPERTIES;
    }


    public static String getString(String property) {
        return getString(property, "");
    }

    public static String getString(String property, String defaultVal) {
        Map<String, Object> profileProperties = loadConfig();
        if (defaultVal == null) defaultVal = "";
        return MapFieldUtil.getMapField(profileProperties, property, defaultVal).toString();
    }

    public static Integer getInteger(String property) {
        return getInteger(property, 0);
    }

    public static Integer getInteger(String property, Integer defaultVal) {
        Map<String, Object> profileProperties = loadConfig();
        if (defaultVal == null) defaultVal = 0;
        return Integer.parseInt(MapFieldUtil.getMapField(profileProperties, property, defaultVal).toString());
    }


    public static Map<String, Object> getMap(String property) {
        return getMap(property, "");
    }

    public static Map<String, Object> getMap(String property, Object defaultVal) {
        Map<String, Object> profileProperties = loadConfig();
        if (defaultVal == null||"".equals(defaultVal)) defaultVal = new HashMap<>();
        return (Map<String, Object>) MapFieldUtil.getMapField(profileProperties, property, defaultVal);
    }

    public static void main(String[] args) {
        Map<String, Object> stringObjectMap = loadConfig();
        System.out.println(stringObjectMap);

    }
}
