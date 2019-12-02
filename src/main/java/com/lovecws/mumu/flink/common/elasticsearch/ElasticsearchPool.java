package com.lovecws.mumu.flink.common.elasticsearch;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: es连接池
 * @date 2018-06-08 20:58
 */
public class ElasticsearchPool implements Serializable {

    private static final Logger log = Logger.getLogger(ElasticsearchPool.class);
    private static final Map<String, GenericObjectPool<ElasticsearchClient>> ELASTICSEARCH_POOL_MAP = new ConcurrentHashMap<>();
    private EsConfig esConfig;

    public ElasticsearchPool(EsConfig esConfig) {
        this.esConfig = esConfig;
        //池子id
        String poolId = esConfig.getHost() + "_" + esConfig.getPort() + "_" + esConfig.getClusterName();
        GenericObjectPool<ElasticsearchClient> pool = ELASTICSEARCH_POOL_MAP.get(poolId);
        if (pool == null) {
            synchronized (ElasticsearchPool.class) {
                pool = ELASTICSEARCH_POOL_MAP.get(poolId);
                if (pool == null) {
                    GenericObjectPoolConfig config = new GenericObjectPoolConfig();
                    config.setMaxIdle(esConfig.getPoolMaxIdle());
                    config.setMaxTotal(esConfig.getPoolMaxNum());
                    config.setMinIdle(esConfig.getPoolMinIdle());
                    config.setTestOnReturn(esConfig.isPoolTestReturn());
                    config.setTestOnBorrow(esConfig.isPoolTestBorrow());
                    config.setTestOnCreate(esConfig.isPoolTestCreate());
                    GenericObjectPool genericObjectPool = new GenericObjectPool(new ElasticsearchPooledObjectFactory(esConfig), config);
                    ELASTICSEARCH_POOL_MAP.put(poolId, genericObjectPool);
                    log.info("initalize elasticsearch pool host[" + esConfig.getHost() + "],port[" + esConfig.getPort() + "],clusterName[" + esConfig.getClusterName() + "]");
                }
            }
        }
    }

    public ElasticsearchClient buildClient() {
        try {
            GenericObjectPool<ElasticsearchClient> pool = ELASTICSEARCH_POOL_MAP.get(getPoolId());
            return pool.borrowObject();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException();
        }
    }

    public void removeClient(ElasticsearchClient client) {
        if (client == null) {
            return;
        }
        GenericObjectPool<ElasticsearchClient> pool = ELASTICSEARCH_POOL_MAP.get(getPoolId());
        pool.returnObject(client);
    }

    public void close() {
        GenericObjectPool<ElasticsearchClient> pool = ELASTICSEARCH_POOL_MAP.get(getPoolId());
        try {
            pool.clear();
            pool.close();
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage());
        }
    }


    private String getPoolId() {
        return esConfig.getHost() + "_" + esConfig.getPort() + "_" + esConfig.getClusterName();
    }
}
