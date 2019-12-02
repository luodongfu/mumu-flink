package com.lovecws.mumu.flink.common.redis;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * ClassName: JedisPoolFactory <br/>
 * Function: redis缓存工厂类. <br/>
 * date: 2014-2-4 上午11:59:56 <br/>
 *
 * @author Alex
 * @since JDK 1.6
 */
public class JedisPoolFactory {

    private JedisPool pool = null;
    private JedisPoolConfig config;
    private static final Logger log = Logger.getLogger(JedisPoolFactory.class);

    private RedisConfig redisConfig;

    public JedisPoolFactory(RedisConfig redisConfig) {
        this.redisConfig = redisConfig;
    }

    public synchronized JedisPool getInstance() {
        return getInstance(redisConfig);
    }

    public synchronized JedisPool getInstance(RedisConfig redisConfig) {
        if (pool == null || pool.isClosed()) {
            Integer maxActive = redisConfig.getMaxActive();
            Integer maxIdle = redisConfig.getMaxIdle();
            Long maxWait = redisConfig.getMaxWait();
            boolean testOnBorrow = redisConfig.isTestOnBorrow();
            boolean testOnReturn = redisConfig.isTestOnReturn();
            String ip = redisConfig.getIp();
            Integer port = redisConfig.getPort();
            Integer timeout = redisConfig.getTimeout();
            String password = redisConfig.getPassword();
            int index = redisConfig.getIndex();

            config = new JedisPoolConfig();
            config.setMaxTotal(maxActive);
            config.setMaxIdle(maxIdle);
            config.setMaxWaitMillis(maxWait);
            config.setTestOnBorrow(testOnBorrow);
            config.setTestOnReturn(testOnReturn);
            config.setMaxWaitMillis(timeout);

            if (StringUtils.isNotBlank(password)) {
                pool = new JedisPool(config, ip, port, timeout, password, index);
            } else {
                pool = new JedisPool(config, ip, port, timeout, null, index);
            }
            /*if (pool.getResource().isConnected()) {
                log.info("redis连接池初始化完毕...");
            }*/
        }
        return pool;
    }

    /**
     * author by : ganliang
     * desc : 如果redis初始化程序还没有执行完毕 就会返回一个null 的jedispool
     *
     * @return
     */
    public JedisPool getPool() {
        return getInstance();
    }

    public void setPool(JedisPool pool) {
        this.pool = pool;
    }
}
