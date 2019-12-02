package com.lovecws.mumu.flink.streaming.common.handler;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.config.ConfigProperties;
import com.lovecws.mumu.flink.common.redis.JedisUtil;
import com.lovecws.mumu.flink.common.util.HttpClientUtil;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.JedisCommands;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@ModularAnnotation(type = "handler", name = "ipbase")
public class IpbaseHandler implements Serializable {

    private String url;
    //缓存前缀 默认SYSLOG_IPBASE_IPBASE
    private String prefix;
    //缓存过期时间 默认一天
    private int expire;
    //分桶数量
    private int bucket;

    public IpbaseHandler() {
        url = ConfigProperties.getString("streaming.defaults.ipbase.url", "localhost");
        prefix = ConfigProperties.getString("streaming.defaults.ipbase.prefix", "INDUSTRY_DATAPROCESSING_IPBASE");
        expire = ConfigProperties.getInteger("streaming.defaults.ipbase.expire", 86440);
        bucket = ConfigProperties.getInteger("streaming.defaults.ipbase.bucket", 24);
    }

    /**
     * 获取到公司企业名称
     *
     * @param ip ip
     * @return
     */
    public Map<String, Object> handler(String ip) {
        JedisCommands jedis = JedisUtil.getJedis();
        if (StringUtils.isEmpty(ip)) return new HashMap<>();
        Map<String, Object> cacheMap = new HashMap<>();
        //计算redis的hash名称
        String hashKey = prefix;
        if (bucket > 0) {
            long mode = Math.abs(ip.hashCode()) % bucket;
            hashKey = prefix + "_" + String.valueOf(mode);
        }
        Object ipBaseInfo = null;
        try {
            //从缓存获取数据
            ipBaseInfo = jedis.hget(hashKey, ip);
            if (ipBaseInfo != null && StringUtils.isNotEmpty(ipBaseInfo.toString())) {
                cacheMap = JSON.parseObject(ipBaseInfo.toString(), Map.class);
            } else {
                cacheMap = getIpbase(url, ip);
                Boolean exists = jedis.exists(hashKey);
                jedis.hset(hashKey, ip, JSON.toJSONString(cacheMap));
                //缓存数据
                if (!exists) jedis.expire(hashKey, expire);
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage() + "[" + ipBaseInfo + "]");
        } finally {
            JedisUtil.close(jedis);
        }
        return cacheMap;
    }

    /**
     * 查询ip基准信息
     *
     * @param url url地址信息
     * @param ip  ip地址信息
     * @return {"wgip":"0","city":"430400","syfs":1,"county":"430408","jtiplx":2,"usingUnitIndustryType":"7","ispName":"湖南联通","ymtzfwq":0,"usingUnitLinkman":"张子龙","province":"430000","usingUnitDwfl":13,"id":226329008,"operatorsId":"2","addr":"湖南省衡阳市蒸湘区芙蓉路14号","ipUsedType":"1","endIp":"110.53.72.255","usingUnitTel":"15980822783","fpfs":1,"beginIp":"110.53.72.0","endIpValue":1848985855,"usingUnitName":"351-广州恒汇网络通信有限公司","ispJyxkzbh":"A2.B1.B2-20090003","benginIpValue":1848985600,"ispId":"1890","usingUnitXzjb":0,"ipType":"1","usingUnitType":"4"}
     */
    public Map<String, Object> getIpbase(String url, String ip) {
        String resp = HttpClientUtil.get(url + "?ip=" + ip);
        if (resp == null || "".equals(resp)) return new HashMap<>();

        List<Map> results = JSON.parseObject(resp, List.class);
        return results == null || results.size() == 0 ? new HashMap<>() : results.get(0);
    }
}
