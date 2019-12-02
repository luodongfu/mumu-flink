package com.lovecws.mumu.flink.streaming.sink;

import com.lovecws.mumu.flink.common.annotation.EsField;
import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.elasticsearch.*;
import com.lovecws.mumu.flink.common.util.DateUtils;
import com.lovecws.mumu.flink.common.util.MapFieldUtil;
import com.lovecws.mumu.flink.common.util.StorageFilterUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.elasticsearch.Version;

import java.lang.reflect.Field;
import java.util.*;

/**
 * @program: trunk
 * @description: es数据存储
 * @author: 甘亮
 * @create: 2019-11-05 10:54
 **/
@Slf4j
@ModularAnnotation(type = "sink", name = "elasticsearch")
public class ElasticsearchSink extends RichSinkFunction<Object> implements BaseSink {
    private String templateName;
    private boolean templateReCreate;
    private String indexName;
    private String indexStragety;
    private String aliasName;
    private String typeName;

    private ElasticsearchBulk elasticsearchBulk;
    private ElasticsearchIndex elasticsearchIndex;
    private volatile boolean init = false;

    //es存储是否启用scriptUpsert插入更新机制
    private boolean scriptUpsert;
    //数量自增字段
    private String scriptCounterField;
    //更新字段
    private String scriptUpdateField;
    //更新标识字段
    private String updateFlagField;
    //过滤规则
    private String filter;

    private EsConfig esConfig;

    public ElasticsearchSink(Map<String, Object> configMap) {
        if (configMap == null) {
            configMap = new HashMap<>();
        }
        filter = MapUtils.getString(configMap, "filter", "").toString();

        esConfig = new EsConfig();
        String host = MapUtils.getString(configMap, "host", "").toString();
        if (StringUtils.isEmpty(host)) {
            host = MapFieldUtil.getMapField(configMap, "defaults.elasticsearch.host").toString();
        }
        String port = MapUtils.getString(configMap, "port", "").toString();
        if (StringUtils.isEmpty(port)) {
            port = MapFieldUtil.getMapField(configMap, "defaults.elasticsearch.port").toString();
        }
        String clusterName = MapUtils.getString(configMap, "clusterName", "").toString();
        if (StringUtils.isEmpty(clusterName)) {
            clusterName = MapFieldUtil.getMapField(configMap, "defaults.elasticsearch.clusterName").toString();
        }
        if (StringUtils.isEmpty(host) || StringUtils.isEmpty(port) || StringUtils.isEmpty(clusterName)) {
            throw new IllegalArgumentException("elasticsearch host|port|clusterName is not allow null");
        }
        esConfig.setPort(port);
        esConfig.setHost(host);
        esConfig.setClusterName(clusterName);
        esConfig.setPoolMaxNum(MapUtils.getIntValue(configMap, "poolMaxNum", 10));
        esConfig.setInsertType(MapUtils.getString(configMap, "insertType", "sync"));
        esConfig.setInsertSleep(MapUtils.getIntValue(configMap, "insertSleep", 0));
        esConfig.setNumberShards(MapUtils.getIntValue(configMap, "shard", 3));
        esConfig.setNumberReplicas(MapUtils.getIntValue(configMap, "replation", 0));
        esConfig.setTotalShardsPerNode(MapUtils.getIntValue(configMap, "totalShardsPerNode", 2));
        esConfig.setDisableAllocation(MapUtils.getBooleanValue(configMap, "disableAllocation", false));

        this.templateName = MapUtils.getString(configMap, "template", "").toString();
        this.templateReCreate = MapUtils.getBooleanValue(configMap, "templateReCreate", true);
        this.indexName = MapUtils.getString(configMap, "index", "").toString();
        this.indexStragety = MapUtils.getString(configMap, "indexStragety", "day").toString();
        this.aliasName = MapUtils.getString(configMap, "alias", "").toString();
        this.typeName = MapUtils.getString(configMap, "type", this.indexName).toString();

        this.scriptUpsert = MapUtils.getBooleanValue(configMap, "scriptUpsert", false);
        this.scriptCounterField = MapUtils.getString(configMap, "scriptCounterField", "");
        this.scriptUpdateField = MapUtils.getString(configMap, "scriptUpdateField", "");
        this.updateFlagField = MapUtils.getString(configMap, "updateFlagField", "");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        log.info("elasticsearch sink open....");

        ElasticsearchPool pool = new ElasticsearchPool(esConfig);
        elasticsearchBulk = new ElasticsearchBulk(esConfig, pool);
        elasticsearchIndex = new ElasticsearchIndex(esConfig, pool);
    }

    @Override
    public void close() throws Exception {
        super.close();
        log.info("elasticsearch sink close....");
        elasticsearchBulk.getPool().close();
        elasticsearchIndex.getPool().close();
    }

    @Override
    public void invoke(Object object, Context context) throws Exception {
        try {
            if (object == null) return;
            //上层传递的数据有可能是集合
            List<Object> datas = new ArrayList<>();
            if (object instanceof List) {
                datas.addAll((List) object);
            } else {
                datas.add(object);
            }
            init(datas.get(0));

            List<Map<String, Object>> _datas = new ArrayList<>();
            for (Object data : datas) {
                Map<String, Object> _map = null;
                if (data instanceof Map) {
                    _map = (Map<String, Object>) data;
                } else {
                    _map = getMapFieldValueByAnnotation(data);
                }
                //是否过滤该条数据
                if (StorageFilterUtil.filter(_map, filter)) {
                    continue;
                }
                _datas.add(_map);
            }
            if (_datas.size() > 1)
                log.info("elasticsearchSinkInvoke:{index:" + getIndexName() + ",aliasName:" + aliasName + ",indexStragety:" + indexStragety + ",size:" + _datas.size() + "}");

            //插入更新
            if (scriptUpsert) {
                elasticsearchBulk.scriptUpserts(getIndexName(), typeName, _datas, scriptCounterField, scriptUpdateField);
            }
            //数据插入或全字段更新
            else {
                elasticsearchBulk.bulk(getIndexName(), typeName, _datas, updateFlagField);
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        }
    }

    /**
     * es初始化 创建es模板或者索引
     *
     * @param object 对象
     */
    private synchronized void init(Object object) {
        try {
            //如果索引已经初始化 则不需要再次初始化
            if (init) {
                return;
            }
            //获取到索引的映射
            List<ElasticsearchMapping.MappingEntity> mappings = getMappings(object);

            //启动索引模板
            if (StringUtils.isNotEmpty(this.templateName)) {
                elasticsearchIndex.createTemplate(this.templateName, this.aliasName, this.typeName, mappings, templateReCreate);
            } else {
                elasticsearchIndex.createIndex(this.indexName, this.aliasName, this.typeName, mappings);
            }
            init = true;
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        }
    }

    @Override
    public SinkFunction<Object> getSinkFunction() {
        return this;
    }

    /**
     * 获取到索引名称
     *
     * @return
     */
    public String getIndexName() {
        //es创建索引的策略，fixed固定索引，day按照日转件索引，week按照周创建索引，month按照月创建索引，year按照年来创建索引，默认fixed
        String finalIndexName;
        switch (indexStragety.toLowerCase()) {
            case "fixed":
                finalIndexName = this.indexName;
                break;
            case "day":
                finalIndexName = this.indexName + "_" + DateUtils.formatDate(new Date(), "yyyyMMdd");
                break;
            case "week":
                finalIndexName = this.indexName + "_" + DateUtils.getDateWeekStartStr(new Date(), "yyyyMMdd");
                break;
            case "month":
                finalIndexName = this.indexName + "_" + DateUtils.formatDate(new Date(), "yyyyMM");
                break;
            case "year":
                finalIndexName = this.indexName + "_" + DateUtils.formatDate(new Date(), "yyyy");
                break;
            default:
                throw new IllegalArgumentException("illegal storage es index strategy");
        }
        return finalIndexName;
    }

    /**
     * 获取到索引的映射关系
     *
     * @return
     */
    public List<ElasticsearchMapping.MappingEntity> getMappings(Object object) {
        List<ElasticsearchMapping.MappingEntity> mappingEntities = new ArrayList<>();
        //解析map的key 通过key来创建索引
        if (object instanceof Map) {
            List<Map<String, Object>> keyMaps = MapFieldUtil.getFlatMapKeys((Map<String, Object>) object);
            for (Map<String, Object> map : keyMaps) {
                String name = map.get("name").toString();
                if ("_uid".equalsIgnoreCase(name) || "_id".equalsIgnoreCase(name))
                    continue;

                String type = map.get("type").toString();
                if (type.equalsIgnoreCase("string")) {
                    type = "keyword";
                } else if (Arrays.asList("integer", "long", "float").contains(type.toLowerCase())) {
                    type = "long";
                }

                if (Version.CURRENT.major == 6) {
                    mappingEntities.add(new ElasticsearchMapping.MappingEntity(name, type, "false"));
                } else {
                    mappingEntities.add(new ElasticsearchMapping.MappingEntity(name, type, null));
                }
            }
        }
        //解析EsField注解字段信息，获取保存es的字段、类型、是否分词解析
        else {
            for (Field field : FieldUtils.getAllFields(object.getClass())) {
                field.setAccessible(true);
                EsField annotation = field.getAnnotation(EsField.class);
                if (annotation == null) {
                    continue;
                }
                if (Version.CURRENT.major == 6) {
                    String analyze = annotation.analyze();
                    if ("not_analyzed".equalsIgnoreCase(analyze)) {
                        analyze = "true";
                    } else {
                        analyze = "true";
                    }
                    mappingEntities.add(new ElasticsearchMapping.MappingEntity(annotation.name(), annotation.type(), analyze, annotation.format()));
                } else {
                    mappingEntities.add(new ElasticsearchMapping.MappingEntity(annotation.name(), annotation.type(), null, annotation.format()));
                }
            }
        }
        return mappingEntities;
    }

    /**
     * 根据注解获取指定的map
     *
     * @param obj 对象
     * @return
     */
    public Map<String, Object> getMapFieldValueByAnnotation(Object obj) {
        Map<String, Object> _sourceMap = new HashMap<String, Object>();
        Field[] allFields = FieldUtils.getFieldsWithAnnotation(obj.getClass(), EsField.class);
        if (allFields == null || allFields.length == 0) {
            for (Field field : FieldUtils.getAllFields(obj.getClass())) {
                try {
                    field.setAccessible(true);
                    Object o = field.get(obj);
                    String type = field.getType().getSimpleName();
                    if ("string".equalsIgnoreCase(type) || "keyword".equalsIgnoreCase(type) || "text".equalsIgnoreCase(type)) {
                        if (o == null) o = "";
                    }
                    if ("int".equalsIgnoreCase(type) || "long".equalsIgnoreCase(type) || "float".equalsIgnoreCase(type)) {
                        if (o == null) o = 0;
                    }
                    //日期数据格式化
                    if ("date".equalsIgnoreCase(type)) {
                        try {
                            o = DateUtils.formatDate((Date) o, "yyyy-MM-dd HH:mm:ss");
                        } catch (Exception e) {
                            log.error(o.toString() + "==转换为时间格式错误", e);
                        }
                    }
                    if (o != null) {
                        _sourceMap.putAll(MapFieldUtil.setMapField(_sourceMap, field.getName(), o));
                    }
                } catch (Exception ex) {
                    log.error(ex.getLocalizedMessage(), ex);
                }
            }
        } else {
            for (Field field : allFields) {
                try {
                    field.setAccessible(true);
                    EsField annotation = field.getAnnotation(EsField.class);
                    Object o = field.get(obj);
                    if (o == null) {
                        String type = annotation.type();
                        if ("string".equalsIgnoreCase(type) || "keyword".equalsIgnoreCase(type) || "text".equalsIgnoreCase(type)) {
                            o = "";
                        }

                        if ("int".equalsIgnoreCase(type) || "long".equalsIgnoreCase(type) || "float".equalsIgnoreCase(type)) {
                            o = 0;
                        }
                    } else {
                        //日期数据格式化
                        if (StringUtils.isNotEmpty(annotation.format()) && "date".equalsIgnoreCase(annotation.type())) {
                            try {
                                o = DateUtils.formatDate((Date) o, annotation.format());
                            } catch (Exception e) {
                                log.error(o.toString() + "==转换为时间格式错误", e);
                            }
                        }
                    }
                    if (o != null) {
                        _sourceMap.putAll(MapFieldUtil.setMapField(_sourceMap, annotation.name(), o));
                    }
                } catch (Exception ex) {
                    log.error(ex.getLocalizedMessage(), ex);
                }
            }
        }
        return _sourceMap;
    }
}
