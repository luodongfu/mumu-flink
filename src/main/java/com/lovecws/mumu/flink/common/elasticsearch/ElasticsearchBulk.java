package com.lovecws.mumu.flink.common.elasticsearch;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: bulk操作
 * @date 2018-06-03 18:24
 */
public class ElasticsearchBulk implements Serializable {

    private static final Logger log = Logger.getLogger(ElasticsearchBulk.class);

    private ElasticsearchPool pool;
    private EsConfig esConfig;

    public ElasticsearchBulk(EsConfig esConfig) {
        this(esConfig, null);
    }

    public ElasticsearchBulk(EsConfig esConfig, ElasticsearchPool elasticsearchPool) {
        this.esConfig = esConfig;
        this.pool = elasticsearchPool;
        if (this.pool == null) pool = new ElasticsearchPool(esConfig);
    }

    public ElasticsearchPool getPool() {
        if (pool == null) pool = new ElasticsearchPool(esConfig);
        return pool;
    }

    /**
     * 创建文档
     *
     * @param indexName 索引名称
     * @param typeName  类型名称
     * @param valueMap  值映射
     */
    public void index(String indexName, String typeName, Map<String, Object> valueMap) {
        ElasticsearchClient elasticsearchClient = getPool().buildClient();
        TransportClient transportClient = elasticsearchClient.client();
        try {
            String key = "";
            if (valueMap.get("id") != null || valueMap.get("key") != null) {
                key = valueMap.getOrDefault("id", valueMap.get("key")).toString();
            }
            if (StringUtils.isEmpty(key)) {
                key = UUID.randomUUID().toString().replace("-", "");
            }
            IndexRequestBuilder indexRequestBuilder = transportClient.prepareIndex();
            indexRequestBuilder.setIndex(indexName);
            indexRequestBuilder.setType(typeName);
            indexRequestBuilder.setId(key);
            indexRequestBuilder.setOpType(IndexRequest.OpType.INDEX);
            indexRequestBuilder.setRouting(key);
            indexRequestBuilder.setSource(ElasticsearchMapping.content(valueMap));
            IndexResponse indexResponse = indexRequestBuilder.get();
            if (indexResponse.status().getStatus() == 200) {
                log.info("文档创建成功！");
            } else {
                indexResponse.writeTo(new OutputStreamStreamOutput(System.out));
            }
        } catch (Exception e) {
            log.error("", e);
            e.printStackTrace();
        } finally {
            getPool().removeClient(elasticsearchClient);
        }
    }

    /**
     * 批量插入
     *
     * @param indexName 索引名称
     * @param typeName  类型名称
     * @param values    值映射集合
     */
    public void bulk(String indexName, String typeName, List<Map<String, Object>> values, String updateFlagField) {

        ElasticsearchClient elasticsearchClient = getPool().buildClient();
        TransportClient transportClient = elasticsearchClient.client();
        try {
            BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();
            for (Map<String, Object> valueMap : values) {
                String key = "";
                if (StringUtils.isNotEmpty(updateFlagField)) {
                    key = valueMap.get(updateFlagField).toString();
                } else {
                    if (valueMap.get("key") != null) {
                        key = valueMap.get("key").toString();
                        valueMap.remove("key");
                    }
                    if (valueMap.get("_id") != null) {
                        key = valueMap.get("_id").toString();
                        valueMap.remove("_id");
                    }
                    if (valueMap.get("_uid") != null) {
                        key = valueMap.get("_uid").toString();
                        valueMap.remove("_uid");
                    }
                    if (StringUtils.isEmpty(key)) {
                        key = UUID.randomUUID().toString().replace("-", "");
                    }
                }
                IndexRequestBuilder indexRequestBuilder = transportClient.prepareIndex();
                indexRequestBuilder.setIndex(indexName);
                indexRequestBuilder.setType(typeName);
                indexRequestBuilder.setId(key);
                indexRequestBuilder.setOpType(IndexRequest.OpType.INDEX);
                indexRequestBuilder.setSource(ElasticsearchMapping.content(valueMap));
                bulkRequestBuilder.add(indexRequestBuilder);
            }
            String insertType = esConfig.getInsertType();
            //同步插入数据
            if ("sync".equalsIgnoreCase(insertType)) {
                BulkResponse bulkItemResponses = bulkRequestBuilder.get();
                if (bulkItemResponses.hasFailures()) {
                    log.error(bulkItemResponses.getItems()[0].getFailureMessage());
                } else {
                    log.info("indexName[" + indexName + "] ,typeName [" + typeName + "] bulk insert [" + values.size() + "] success!");
                }
            }
            //异步插入数据
            else if ("async".equalsIgnoreCase(insertType)) {
                bulkRequestBuilder.execute(new ActionListener<BulkResponse>() {
                    @Override
                    public void onResponse(BulkResponse bulkItemResponses) {
                        log.info("indexName[" + indexName + "] ,typeName [" + typeName + "] bulk insert [" + values.size() + "] success!");
                    }

                    @Override
                    public void onFailure(Exception e) {
                        log.error(e.getLocalizedMessage(), e);
                    }
                });
            }
            //插入数据完成之后休眠固定的时间，防止es插入数据太频繁导致es崩溃。
            Integer sleepTime = esConfig.getInsertSleep();
            if (sleepTime > 0) {
                TimeUnit.SECONDS.sleep(sleepTime);
            }
        } catch (Exception e) {
            log.error("bulkError", e);
        } finally {
            getPool().removeClient(elasticsearchClient);
        }
    }

    /**
     * 文档不存在则创建、否则更新
     *
     * @param indexName 索引名称
     * @param typeName  类型名称
     * @param id        主键
     * @param valueMap  值映射
     */
    public int upsert(String indexName, String typeName, String id, Map<String, Object> valueMap) {
        ElasticsearchClient elasticsearchClient = getPool().buildClient();
        TransportClient transportClient = elasticsearchClient.client();
        if (valueMap == null) return RestStatus.BAD_REQUEST.getStatus();
        try {
            UpdateRequest updateRequest = new UpdateRequest(indexName, typeName, id)
                    .doc(ElasticsearchMapping.content(valueMap))
                    //.docAsUpsert(true)
                    .upsert(ElasticsearchMapping.content(valueMap));
            UpdateResponse updateResponse = transportClient.update(updateRequest).get();
            return updateResponse.status().getStatus();
        } catch (Exception e) {
            log.error(e);
        } finally {
            getPool().removeClient(elasticsearchClient);
        }
        return RestStatus.INTERNAL_SERVER_ERROR.getStatus();
    }

    /**
     * script更新
     *
     * @param indexName    索引名称
     * @param typeName     类型名称
     * @param id           主键
     * @param scriptSource 脚本
     * @param scriptParams 参数
     *                     POST test/_update/1
     *                     {
     *                     "script" : {
     *                     "source": "ctx._source.counter += params.count)",
     *                     "lang": "painless",
     *                     "params" : {
     *                     "tag" : "blue"
     *                     }
     *                     }
     *                     }
     */
    public int script(String indexName, String typeName, String id, String scriptSource, Map<String, Object> scriptParams) {
        ElasticsearchClient elasticsearchClient = getPool().buildClient();
        TransportClient transportClient = elasticsearchClient.client();
        try {
            UpdateRequest updateRequest = new UpdateRequest(indexName, typeName, id)
                    .script(new Script(ScriptType.INLINE, "painless", scriptSource, scriptParams));
            UpdateResponse updateResponse = transportClient.update(updateRequest).get();
            return updateResponse.status().getStatus();
        } catch (Exception e) {
            log.error(e);
        } finally {
            getPool().removeClient(elasticsearchClient);
        }
        return RestStatus.INTERNAL_SERVER_ERROR.getStatus();
    }

    /**
     * 当文档不存在的时候 直接添加文档 valueMap
     *
     * @param indexName    索引名称
     * @param typeName     索引类型
     * @param id           索引id
     * @param scriptSource 脚本
     * @param scriptParams 脚本参数
     * @param valueMap     新添加的文档值
     *                     {
     *                     "script" : {
     *                     "source": "ctx._source.counter += params.count",
     *                     "lang": "painless",
     *                     "params" : {
     *                     "count" : 4
     *                     }
     *                     },
     *                     "upsert" : {
     *                     "counter" : 1
     *                     }
     *                     }
     * @return
     */
    public int scriptUpsert(String indexName, String typeName, String id, String scriptSource, Map<String, Object> scriptParams, Map<String, Object> valueMap) {
        ElasticsearchClient elasticsearchClient = getPool().buildClient();
        TransportClient transportClient = elasticsearchClient.client();
        try {
            UpdateRequest updateRequest = new UpdateRequest(indexName, typeName, id)
                    .script(new Script(ScriptType.INLINE, "painless", scriptSource, scriptParams))
                    .upsert(valueMap)
                    .scriptedUpsert(false);//设为true 当文档是否存在都执行script脚本
            UpdateResponse updateResponse = transportClient.update(updateRequest).get();
            return updateResponse.status().getStatus();
        } catch (Exception e) {
            log.error(e);
        } finally {
            getPool().removeClient(elasticsearchClient);
        }
        return RestStatus.INTERNAL_SERVER_ERROR.getStatus();
    }

    public void scriptUpserts(String indexName, String typeName, List<Map<String, Object>> eventDatas, String scriptCounterField, String scriptUpdateField) {
        if (StringUtils.isEmpty(scriptCounterField) && StringUtils.isEmpty(scriptUpdateField)) {
            throw new IllegalArgumentException();
        }

        if (StringUtils.isEmpty(scriptCounterField)) {
            scriptCounterField = "";
        }

        if (StringUtils.isEmpty(scriptUpdateField)) {
            scriptUpdateField = "";
        }

        ElasticsearchClient elasticsearchClient = getPool().buildClient();
        TransportClient transportClient = elasticsearchClient.client();
        try {
            for (Map<String, Object> map : eventDatas) {
                String id = MapUtils.getString(map, "id", "");
                if (StringUtils.isEmpty(id)) {
                    id = MapUtils.getString(map, "_id", "");
                    map.remove("_id");
                }
                if (StringUtils.isEmpty(id)) {
                    id = MapUtils.getString(map, "_uid", "");
                    map.remove("_uid");
                }
                if (StringUtils.isEmpty(id)) {
                    id = UUID.randomUUID().toString().replace("-", "");
                }

                Map<String, Object> scriptParams = new HashMap<>();
                StringBuffer scriptSource = new StringBuffer();
                for (String counterField : scriptCounterField.split(",")) {
                    scriptSource.append("ctx._source." + counterField + " += params." + counterField + ";");
                    scriptParams.put(counterField, map.getOrDefault(counterField, "1"));
                }
                for (String updateField : scriptUpdateField.split(",")) {
                    scriptSource.append("ctx._source." + updateField + " = params." + updateField + ";");
                    scriptParams.put(updateField, map.get(updateField));
                }

                UpdateRequest updateRequest = new UpdateRequest(indexName, typeName, id)
                        .script(new Script(ScriptType.INLINE, "painless", scriptSource.toString(), scriptParams))
                        .upsert(map)
                        .scriptedUpsert(false);//设为true 当文档是否存在都执行script脚本
                UpdateResponse updateResponse = transportClient.update(updateRequest).get();
                log.debug(updateResponse.status().getStatus());
            }
            log.debug("indexName[" + indexName + "] ,typeName [" + typeName + "] script upsert [" + eventDatas.size() + "] success!");
        } catch (Exception e) {
            log.error(e);
        } finally {
            getPool().removeClient(elasticsearchClient);
        }
    }
}
