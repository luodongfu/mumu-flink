package com.lovecws.mumu.flink.streaming.common.cache;

import com.lovecws.mumu.flink.common.redis.JedisUtil;
import com.lovecws.mumu.flink.common.service.atd.SyslogDictService;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.JedisCommands;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SyslogDictCache implements Serializable {

    private SyslogDictService syslogDictService = new SyslogDictService();

    public static String SYSLOG_DICT_PREFIX = "INDUSTRY_DATAPROCESSING_SYSLOGDICT";
    public static int SYSLOG_DICT_CACHE_EXPIRE = 86440;

    /**
     * 获取到公司企业名称
     *
     * @param severity 威胁等级
     * @return
     */
    public String getSyslogSeverity(String severity) {
        if (severity == null) severity = "";

        String hashKey = SYSLOG_DICT_PREFIX + "_SEVERITY";
        Object severityCn = "";
        JedisCommands jedis = JedisUtil.getJedis();
        try {
            //直接从缓存获取 如果缓存存在直接返回，否则读取数据库，将数据写入到缓存之后在返回
            severityCn = jedis.hget(hashKey, severity);
            if (severityCn == null || StringUtils.isEmpty(severityCn.toString())) {
                List<Map<Object, Object>> severitys = syslogDictService.getThreadSeverityDict();
                if (severitys != null && severitys.size() > 0) {
                    Boolean exists = jedis.exists(hashKey);
                    if (exists == null || !exists) {
                        jedis.hset(hashKey, "temp", "");
                        //缓存数据
                        jedis.expire(hashKey, SYSLOG_DICT_CACHE_EXPIRE);
                    }

                    Map<String, String> severityMap = new HashMap<>();
                    severitys.forEach(map -> severityMap.put(map.getOrDefault("name", "").toString(), map.getOrDefault("namecn", "").toString()));
                    jedis.hmset(hashKey, severityMap);
                    severityCn = severityMap.get(severity);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            JedisUtil.close(jedis);
        }
        if (severityCn == null) severityCn = "";
        return severityCn.toString();
    }
}
