package com.lovecws.mumu.flink.streaming.common.cache;

import com.lovecws.mumu.flink.common.redis.JedisUtil;
import com.lovecws.mumu.flink.common.service.atd.SyslogCorpService;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.JedisCommands;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class SyslogCorpCache implements Serializable {

    private SyslogCorpService syslogCorpService=new SyslogCorpService();

    public static String SYSLOG_CORP_PREFIX = "INDUSTRY_DATAPROCESSING_CORP";
    public static int SYSLOG_CORP_CACHE_EXPIRE = 86440;
    public static int SYSLOG_CORP_CACHE_BUCKET = 24;

    /**
     * 获取到公司企业名称
     *
     * @param ipValue ip数字值
     * @return
     */
    public Map<String, Object> getCorpInfo(Long ipValue) {
        Map<String, Object> cacheMap = new HashMap<>();
        //计算redis的hash名称
        String hashKey = SYSLOG_CORP_PREFIX + "DIC";
        if (SYSLOG_CORP_CACHE_BUCKET > 0) {
            long mode = ipValue % SYSLOG_CORP_CACHE_BUCKET;
            hashKey = hashKey + "_" + String.valueOf(mode);
        }
        JedisCommands jedis = JedisUtil.getJedis();
        try {
            //直接从缓存获取 如果缓存存在直接返回，否则读取数据库，将数据写入到缓存之后在返回
            Object ipCorpInfo = jedis.hget(hashKey, ipValue.toString());
            if (ipCorpInfo != null && StringUtils.isNotEmpty(ipCorpInfo.toString())) {
                String[] arr = ipCorpInfo.toString().split(",", -1);
                cacheMap.put("corpid", arr[0]);
                cacheMap.put("corpname", arr[1]);
                cacheMap.put("industry", arr[2]);
                //历史缓存问题
                if (arr.length != 4) {
                    cacheMap.put("corptype", "");
                } else {
                    cacheMap.put("corptype", arr[3]);
                }
            } else {
                Boolean exists = jedis.exists(hashKey);
                if (exists == null || !exists) {
                    jedis.hset(hashKey, "temp", "");
                    //缓存数据
                    jedis.expire(hashKey, SYSLOG_CORP_CACHE_EXPIRE);
                }
                Map<String, Object> corpMap = syslogCorpService.getCorpInfo(ipValue);
                if (corpMap == null) corpMap = new HashMap<>();
                String corpid = corpMap.getOrDefault("corpid", "").toString();
                String corpname = corpMap.getOrDefault("corpname", "").toString();
                String industry = corpMap.getOrDefault("industry", "").toString();
                String corptype = corpMap.getOrDefault("corptype", "").toString();
                cacheMap.put("corpid", corpid);
                cacheMap.put("corpname", corpname);
                cacheMap.put("industry", industry);
                cacheMap.put("corptype", corptype);
                jedis.hset(hashKey, ipValue.toString(), corpid + "," + corpname + "," + industry + "," + corptype);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            log.error(ex.getLocalizedMessage() + "[" + JSON.toJSONString(cacheMap) + "]");
        } finally {
            JedisUtil.close(jedis);
        }
        return cacheMap;
    }
}
