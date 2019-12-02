package com.lovecws.mumu.flink.common.redis;

import com.lovecws.mumu.flink.common.config.ConfigProperties;
import com.lovecws.mumu.flink.common.util.MapFieldUtil;
import redis.clients.jedis.*;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * @program: act-industry-data-common
 * @description: jedis工具类
 * @author: 甘亮
 * @create: 2019-11-07 15:51
 **/
public class JedisUtil {

    private static JedisPool jedisPool = null;
    private static JedisCluster jedisCluster = null;
    private static final Object object = new Object();

    private static Map<String, Object> redisMap = new HashMap<>();
    private static String redisType = "standalone";

    static {
        redisMap = ConfigProperties.getMap("spring.redis");
        redisType = MapFieldUtil.getMapField(redisMap, "type", "standalone").toString();
    }

    public static JedisCommands getJedis() {
        if ("standalone".equalsIgnoreCase(redisType)) {
            if (jedisPool == null) {
                synchronized (object) {
                    if (jedisPool == null) {
                        RedisConfig redisConfig = new RedisConfig();
                        redisConfig.setIp(MapFieldUtil.getMapField(redisMap, "standalone.host").toString());
                        redisConfig.setPort(Integer.parseInt(MapFieldUtil.getMapField(redisMap, "standalone.port").toString()));
                        redisConfig.setIndex(Integer.parseInt(MapFieldUtil.getMapField(redisMap, "standalone.database").toString()));
                        redisConfig.setPassword(MapFieldUtil.getMapField(redisMap, "standalone.password").toString());

                        JedisPoolFactory jedisPoolFactory = new JedisPoolFactory(redisConfig);
                        jedisPool = jedisPoolFactory.getInstance();
                    }
                }
            }
            return jedisPool.getResource();
        } else if ("cluster".equalsIgnoreCase(redisType)) {
            if (jedisCluster == null) {
                synchronized (object) {
                    if (jedisCluster == null) {
                        JedisPoolConfig poolConfig = new JedisPoolConfig();
                        poolConfig.setMaxTotal(100);
                        poolConfig.setMaxIdle(2);
                        poolConfig.setTestOnBorrow(false);
                        poolConfig.setTestOnReturn(false);
                        poolConfig.setMaxWaitMillis(30000);
                        poolConfig.setMaxWaitMillis(1000);

                        Set<HostAndPort> nodes = new LinkedHashSet<HostAndPort>();
                        String clusterNodes = MapFieldUtil.getMapField(redisMap, "cluster.nodes").toString();
                        for (String clusterNode : clusterNodes.split(",")) {
                            String[] host_port = clusterNode.split(":");
                            nodes.add(new HostAndPort(host_port[0], Integer.parseInt(host_port[1])));
                        }
                        jedisCluster = new JedisCluster(nodes, 30000, 3, poolConfig);
                    }
                }
            }
            return jedisCluster;
        }
        throw new IllegalArgumentException();
    }

    public static void close(JedisCommands jedisCommands) {
        if ("standalone".equalsIgnoreCase(redisType)) {
            if (jedisPool != null) ((Jedis) jedisCommands).close();
        }
    }

    public static void destory() {
        try {
            if (jedisPool != null) jedisPool.close();
            if (jedisCluster != null) jedisCluster.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) {
        JedisCommands jedis = getJedis();
        String set = jedis.set("name", "lovecws");
        System.out.println(set);
        close(jedis);
        destory();
    }
}
