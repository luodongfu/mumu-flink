package com.lovecws.mumu.flink.common.elasticsearch;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.settings.Settings;

import java.io.Serializable;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 索引
 * @date 2018-06-03 11:50
 */
public class ElasticsearchIndex implements Serializable {

    private static final Logger log = Logger.getLogger(ElasticsearchIndex.class);

    private ElasticsearchPool pool;
    private EsConfig esConfig;

    public ElasticsearchIndex(EsConfig esConfig) {
        this(esConfig, null);
    }

    public ElasticsearchIndex(EsConfig esConfig, ElasticsearchPool elasticsearchPool) {
        this.esConfig = esConfig;
        this.pool = elasticsearchPool;
        if (this.pool == null) pool = new ElasticsearchPool(esConfig);
    }

    public ElasticsearchPool getPool() {
        if (pool == null) pool = new ElasticsearchPool(esConfig);
        return pool;
    }

    /**
     * 创建索引
     *
     * @param indexName 索引名称
     * @param aliasName 别名名称,支持多个别名使用逗号分隔
     * @param typeName  类型名称
     * @param mappings  字段映射集合
     */
    public boolean createIndex(String indexName, String aliasName, String typeName, List<ElasticsearchMapping.MappingEntity> mappings) {
        ElasticsearchClient elasticsearchClient = getPool().buildClient();
        TransportClient transportClient = elasticsearchClient.client();
        try {
            //判断索引是否存在
            IndicesExistsResponse existsResponse = transportClient.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet();
            if (existsResponse.isExists()) {
                log.info("index [" + indexName + "] exists!");
                return false;
            }
            Settings.Builder settingBuilder = Settings.builder()
                    .put("index.number_of_shards", esConfig.getNumberShards())
                    .put("index.number_of_replicas", esConfig.getNumberReplicas())
                    .put("index.refresh_interval", esConfig.getRefreshInterval());
            /*int totalShardsPerNode = esConfig.getTotalShardsPerNode();
            if (totalShardsPerNode > 0) {
                settingBuilder = settingBuilder
                        .put("index.routing.allocation.total_shards_per_node", totalShardsPerNode)
                        .put("cluster.routing.allocation.disable_allocation", esConfig.isDisableAllocation());
            }*/
            Settings settings = settingBuilder.build();

            CreateIndexRequestBuilder createIndexRequestBuilder = transportClient.admin().indices()
                    .prepareCreate(indexName)
                    .setSettings(settings)
                    .addMapping(typeName, ElasticsearchMapping.mapping(typeName, mappings))
                    .setUpdateAllTypes(true);

            if (StringUtils.isNotEmpty(aliasName)) {
                String[] aliases = aliasName.split(",");
                for (String alias : aliases) {
                    createIndexRequestBuilder.addAlias(new Alias(alias));
                }
            }
            CreateIndexResponse createIndexResponse = createIndexRequestBuilder.execute()
                    .actionGet();
            if (createIndexResponse.isAcknowledged()) {
                log.info("index [" + indexName + "] create success!");
                return true;
            } else {
                createIndexResponse.writeTo(new OutputStreamStreamOutput(System.out));
            }
        } catch (Exception e) {
            log.error(e.getLocalizedMessage(), e);
        } finally {
            getPool().removeClient(elasticsearchClient);
        }
        return false;
    }

    /**
     * 创建模板
     *
     * @param templateName 索引名称
     * @param aliasName    别名名称
     * @param typeName     类型名称
     * @param mappings     字段映射集合
     */
    public boolean createTemplate(String templateName, String aliasName, String typeName, List<ElasticsearchMapping.MappingEntity> mappings, boolean templateReCreate) {
        ElasticsearchClient elasticsearchClient = getPool().buildClient();
        TransportClient transportClient = elasticsearchClient.client();
        try {
            //判断索引模板是否存在
            GetIndexTemplatesResponse templatesResponse = transportClient.admin().indices().getTemplates(new GetIndexTemplatesRequest(templateName)).get();
            if (templatesResponse.getIndexTemplates().size() > 0) {
                log.info("template [" + templateName + "] exists!");
                //索引模板存在 如果重新生成 则先删除在创建
                if (templateReCreate) {
                    transportClient.admin().indices().deleteTemplate(new DeleteIndexTemplateRequest(templateName)).get();
                    log.info("template [" + templateName + "] delete!");
                } else {
                    return false;
                }
            }
            Settings.Builder settingBuilder = Settings.builder()
                    .put("index.number_of_shards", esConfig.getNumberShards())
                    .put("index.number_of_replicas", esConfig.getNumberReplicas())
                    .put("index.refresh_interval", esConfig.getRefreshInterval());

            PutIndexTemplateResponse templateResponse = transportClient.admin().indices()
                    .preparePutTemplate(templateName)
                    .setTemplate(templateName)
                    .setSettings(settingBuilder)
                    .addAlias(new Alias(aliasName))
                    .addMapping(typeName, ElasticsearchMapping.mapping(typeName, mappings))
                    .setCreate(true)
                    .get();
            if (templateResponse.isAcknowledged()) {
                log.info("template [" + templateName + "] create success!");
                return true;
            } else {
                templateResponse.writeTo(new OutputStreamStreamOutput(System.out));
            }
        } catch (Exception e) {
            log.error(e.getLocalizedMessage(), e);
        } finally {
            getPool().removeClient(elasticsearchClient);
        }
        return false;
    }

    /**
     * 判断索引是否存在
     *
     * @param indexName 索引的名称
     * @return
     */
    public boolean exists(String indexName) {
        ElasticsearchClient elasticsearchClient = getPool().buildClient();
        TransportClient transportClient = elasticsearchClient.client();
        try {
            IndicesExistsResponse existsResponse = transportClient.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet();
            return existsResponse.isExists();
        } catch (Exception e) {
            log.error("", e);
            e.printStackTrace();
        } finally {
            getPool().removeClient(elasticsearchClient);
        }
        return false;
    }

    /**
     * 删除索引
     *
     * @param indexName 索引的名称
     */
    public boolean deleteIndex(String indexName) {
        if (!exists(indexName)) {
            log.info("索引[" + indexName + "] 不存在");
            return false;
        }
        ElasticsearchClient elasticsearchClient = getPool().buildClient();
        TransportClient transportClient = elasticsearchClient.client();
        boolean deleteSuccess = false;
        try {
            DeleteIndexResponse deleteIndexResponse = transportClient.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
            if (deleteIndexResponse.isAcknowledged()) {
                log.info("索引[" + indexName + "] 删除成功");
                deleteSuccess = true;
            } else {
                deleteIndexResponse.writeTo(new OutputStreamStreamOutput(System.out));
            }
        } catch (Exception e) {
            log.error("", e);
        } finally {
            getPool().removeClient(elasticsearchClient);
        }
        return deleteSuccess;
    }

    /**
     * 关闭索引
     *
     * @param indexName 索引的名称
     * @return
     */
    public boolean closeIndex(String indexName) {
        ElasticsearchClient elasticsearchClient = getPool().buildClient();
        TransportClient transportClient = elasticsearchClient.client();
        if (!exists(indexName)) {
            log.info("索引[" + indexName + "] 不存在");
            return false;
        }
        boolean closeSuccess = false;
        try {
            CloseIndexResponse closeIndexResponse = transportClient.admin().indices().close(new CloseIndexRequest(indexName)).actionGet();
            log.info(closeIndexResponse.isAcknowledged());
            closeSuccess = true;
        } catch (Exception e) {
            log.error("", e);
            e.printStackTrace();
        } finally {
            getPool().removeClient(elasticsearchClient);
        }
        return closeSuccess;
    }

    /**
     * 打开索引
     *
     * @param indexName 索引的名称
     * @return
     */
    public boolean openIndex(String indexName) {
        if (!exists(indexName)) {
            log.info("索引[" + indexName + "] 不存在");
            return false;
        }
        ElasticsearchClient elasticsearchClient = getPool().buildClient();
        TransportClient transportClient = elasticsearchClient.client();
        boolean openSuccess = false;
        try {
            OpenIndexResponse openIndexResponse = transportClient.admin().indices().open(new OpenIndexRequest(indexName)).actionGet();
            log.info(openIndexResponse.isAcknowledged());
            openSuccess = true;
        } catch (Exception e) {
            log.error("openIndexError", e);
            e.printStackTrace();
        } finally {
            getPool().removeClient(elasticsearchClient);
        }
        return openSuccess;
    }
}
