package com.lovecws.mumu.flink.streaming.common.cache;

import com.lovecws.mumu.flink.common.config.ConfigProperties;
import com.lovecws.mumu.flink.common.model.gynetres.Enterprise;
import com.lovecws.mumu.flink.common.redis.JedisUtil;
import com.lovecws.mumu.flink.common.service.atd.SyslogCorpService;
import com.lovecws.mumu.flink.common.service.gynetres.EnterpriseService;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.JedisCommands;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class MCorpCache implements Serializable {

    private SyslogCorpService corpService = new SyslogCorpService();
    private EnterpriseService enterpriseService = new EnterpriseService();

    private String ENTERPRISE_CORP_PREFIX;
    private String ENTERPRISE_CORP_CACHE_EXPIRE;
    private int ENTERPRISE_CORP_CACHE_BUCKET;

    private String ENTERPRISE_BASE_CORP_PREFIX;
    private String ENTERPRISE_BASE_CORP_CACHE_EXPIRE;
    private int ENTERPRISE_BASE_CORP_CACHE_BUCKET;

    public MCorpCache() {
        ENTERPRISE_CORP_PREFIX = ConfigProperties.getString("streaming.defaults.cache.corpEnterprise.prefix", "INDUSTRY_ENTERPRISE_CORP");
        ENTERPRISE_CORP_CACHE_EXPIRE = ConfigProperties.getString("streaming.defaults.cache.corpEnterprise.expire", "7d");
        ENTERPRISE_CORP_CACHE_BUCKET = ConfigProperties.getInteger("streaming.defaults.cache.corpEnterprise.bucket", 24);

        ENTERPRISE_BASE_CORP_PREFIX = ConfigProperties.getString("streaming.defaults.cache.corpEnterpriseBase.prefix", "INDUSTRY_ENTERPRISE_BASE_CORP");
        ENTERPRISE_BASE_CORP_CACHE_EXPIRE = ConfigProperties.getString("streaming.defaults.cache.corpEnterpriseBase.expire", "7d");
        ENTERPRISE_BASE_CORP_CACHE_BUCKET = ConfigProperties.getInteger("streaming.defaults.cache.corpEnterpriseBase.bucket", 200);
    }

    /**
     * 根据ip获取到企业名称、行业小类、企业性质（从tb_enterprise表中获取）
     *
     * @param ipValue ip数字值
     * @return
     */
    public Map<String, Object> getCorpInfo(Long ipValue) {
        Map<String, Object> cacheMap = new HashMap<>();
        //计算redis的hash名称
        String hashKey = ENTERPRISE_CORP_PREFIX + "DIC";
        if (ENTERPRISE_CORP_CACHE_BUCKET > 0) {
            long mode = ipValue % ENTERPRISE_CORP_CACHE_BUCKET;
            hashKey = hashKey + "_" + String.valueOf(mode);
        }
        JedisCommands jedis = JedisUtil.getJedis();
        try {
            //直接从缓存获取 如果缓存存在直接返回，否则读取数据库，将数据写入到缓存之后在返回
            Object ipCorpInfo = jedis.hget(hashKey, ipValue.toString());
            if (ipCorpInfo != null && StringUtils.isNotEmpty(ipCorpInfo.toString())) {
                String[] arr = ipCorpInfo.toString().split(",", -1);
                cacheMap.put("corpname", arr[0]);
                cacheMap.put("industry", arr[1]);
                //历史缓存问题
                if (arr.length != 3) {
                    cacheMap.put("corptype", "");
                } else {
                    cacheMap.put("corptype", arr[2]);
                }
            } else {
                Boolean exists = jedis.exists(hashKey);
                if (exists == null || !exists) {
                    jedis.hset(hashKey, "temp", "");
                    //缓存数据
                    jedis.expire(hashKey, getExpireTime(ENTERPRISE_CORP_CACHE_EXPIRE));
                }

                LambdaQueryWrapper<Enterprise> queryWrapper = new QueryWrapper<Enterprise>().lambda();
                queryWrapper.le(Enterprise::getBeginipvalue, ipValue).ge(Enterprise::getEndipvalue, ipValue);
                Enterprise enterpriseInfo = enterpriseService.getOne(queryWrapper);
                String companyname = "", subIndustryName = "", companytypecategory = "";
                if (enterpriseInfo != null) {
                    companyname = enterpriseInfo.getCompanyname() == null ? "" : enterpriseInfo.getCompanyname();
                    subIndustryName = enterpriseInfo.getSubIndustryName() == null ? "" : enterpriseInfo.getSubIndustryName();
                    companytypecategory = enterpriseInfo.getCompanytypecategory() == null ? "" : enterpriseInfo.getCompanytypecategory();
                }
                cacheMap.put("corpname", companyname);
                cacheMap.put("industry", subIndustryName);
                cacheMap.put("corptype", companytypecategory);
                jedis.hset(hashKey, ipValue.toString(), companyname + "," + subIndustryName + "," + companytypecategory);
                //缓存数据
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            JedisUtil.close(jedis);
        }
        return cacheMap;
    }

    /**
     * 根据ip获取到企业名称、行业小类、企业性质（从tb_enterprise_base表中获取）
     *
     * @param ipValue ip数字值
     * @return
     */
    public Map<String, Object> getCorpInfoFromDB(Long ipValue) {
        Map<String, Object> cacheMap = new HashMap<>();
        //计算redis的hash名称
        String hashKey = ENTERPRISE_BASE_CORP_PREFIX + "DIC";
        if (ENTERPRISE_BASE_CORP_CACHE_BUCKET > 0) {
            long mode = ipValue % ENTERPRISE_BASE_CORP_CACHE_BUCKET;
            hashKey = hashKey + "_" + String.valueOf(mode);
        }
        JedisCommands jedis = JedisUtil.getJedis();
        try {
            //直接从缓存获取 如果缓存存在直接返回，否则读取数据库，将数据写入到缓存之后在返回
            Object ipCorpInfo = jedis.hget(hashKey, ipValue.toString());
            if (ipCorpInfo != null && StringUtils.isNotEmpty(ipCorpInfo.toString())) {
                String[] arr = ipCorpInfo.toString().split(",", -1);
                cacheMap.put("corpname", arr[0]);
                cacheMap.put("industry", arr[1]);
                //历史缓存问题
                if (arr.length != 3) {
                    cacheMap.put("corptype", "");
                } else {
                    cacheMap.put("corptype", arr[2]);
                }
            } else {
                Boolean exists = jedis.exists(hashKey);
                if (exists == null || !exists) {
                    jedis.hset(hashKey, "temp", "");
                    //缓存数据
                    jedis.expire(hashKey, getExpireTime(ENTERPRISE_CORP_CACHE_EXPIRE));
                }

                Map<String, Object> corpMap = corpService.getCorpInfoFromBD(ipValue);
                if (corpMap == null) corpMap = new HashMap<>();
                String corpname = corpMap.getOrDefault("corpname", "").toString();
                String industry = corpMap.getOrDefault("industry", "").toString();
                String corptype = corpMap.getOrDefault("corptype", "").toString();
                cacheMap.put("corpname", corpname);
                cacheMap.put("industry", industry);
                cacheMap.put("corptype", corptype);
                jedis.hset(hashKey, ipValue.toString(), corpname + "," + industry + "," + corptype);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            JedisUtil.close(jedis);
        }
        return cacheMap;
    }

    private int getExpireTime(String expire) {
        int expireTime = 0;
        int timeCount = Integer.parseInt(expire.replaceAll("\\D+", ""));
        if (expire.endsWith("s")) {
            expireTime = timeCount;
        } else if (expire.endsWith("m")) {
            expireTime = timeCount * 60;
        } else if (expire.endsWith("h")) {
            expireTime = timeCount * 60 * 60;
        } else if (expire.endsWith("d")) {
            expireTime = timeCount * 60 * 60 * 24;
        }
        return expireTime;
    }

}
