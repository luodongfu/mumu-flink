package com.lovecws.mumu.flink.streaming.common.cache;

import com.lovecws.mumu.flink.common.redis.JedisUtil;
import com.lovecws.mumu.flink.common.util.MD5Util;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisCommands;

import java.io.Serializable;
import java.util.Map;

/**
 * @program: act-able
 * @description: 源文件缓存，当源数据为ftp、sftp的时候并且没有修改sftp、ftp的权限，该缓存主要缓存ftp|sftp的文件名称，防止重复解析
 * @author: 甘亮
 * @create: 2019-06-24 11:49
 **/
@Slf4j
public class SourceFileCache implements Serializable {


    /**
     * 文件是否缓存
     *
     * @param key      缓存key
     * @param fileName 文件名称
     * @return
     */
    public boolean hasCache(String key, String fileName) {
        JedisCommands jedis = JedisUtil.getJedis();
        try {
            return jedis.hexists(key, MD5Util.md5(fileName));
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        } finally {
            JedisUtil.close(jedis);
        }
        return false;
    }

    public void caches(String key, Map<String, String> valueMap, int expireInSecond) {
        JedisCommands jedis = JedisUtil.getJedis();
        try {
            Boolean exists = false;
            if (expireInSecond > 0) exists = jedis.exists(key);
            jedis.hmset(key, valueMap);
            if (expireInSecond > 0 && exists != null && !exists) {
                jedis.expire(key, expireInSecond);
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        } finally {
            JedisUtil.close(jedis);
        }
    }
}
