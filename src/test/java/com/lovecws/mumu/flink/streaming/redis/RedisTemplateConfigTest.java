package com.lovecws.mumu.flink.streaming.redis;

import com.lovecws.mumu.flink.common.redis.JedisUtil;
import org.junit.Test;
import redis.clients.jedis.JedisCommands;

import java.util.Set;

/**
 * @program: trunk
 * @description: redis测试
 * @author: 甘亮
 * @create: 2019-09-11 10:40
 **/
public class RedisTemplateConfigTest {

    @Test
    public void keys() {
        JedisCommands jedis = JedisUtil.getJedis();
        Set keys = jedis.hkeys("*");
        keys.forEach(key -> System.out.println(key));
    }
}
