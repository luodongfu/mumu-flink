package com.lovecws.mumu.flink.streaming.source;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.elasticsearch.ElasticsearchQuery;
import com.lovecws.mumu.flink.common.elasticsearch.EsConfig;
import com.lovecws.mumu.flink.common.util.MapFieldUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.List;
import java.util.Map;

/**
 * @program: act-able
 * @description: 从elasticsearch读取数据。使用滚屏方式读取数据，默认按照_id进行排序，记录最后的排序值，下次从该排序值进行查询。
 * @author: 甘亮
 * @create: 2019-11-20 16:47
 **/
@Slf4j
@ModularAnnotation(type = "source", name = "elasticsearch")
public class ElasticsearchSource extends AbstractRichFunction implements BaseSource, SourceFunction<Object>, CheckpointedFunction {

    private ElasticsearchQuery elasticsearchQuery;//es客户端

    //排序字段和字段值，滚屏按照某一个字段(字段值尽量唯一，默认为es的_id)进行排序(默认升序asc)，如果scroll过期或者中断，可以从这个中断的值进行滚屏。
    private String sortField;
    private String sortOrder;

    //每次滚屏查询的数据量
    private int batchSize;

    //查询字段和字段值,目前查询只支持termquery的must和mustnot，如果不等于，请在queryFieldValue加上！
    private String queryFieldName;
    private String queryFieldValue;

    public ElasticsearchSource(Map<String, Object> configMap) {
        String host = MapUtils.getString(configMap, "host", host = MapFieldUtil.getMapField(configMap, "defaults.elasticsearch.host").toString());
        String port = MapUtils.getString(configMap, "port", MapFieldUtil.getMapField(configMap, "defaults.elasticsearch.port").toString());
        String clusterName = MapUtils.getString(configMap, "clusterName", MapFieldUtil.getMapField(configMap, "defaults.elasticsearch.clusterName").toString());
        if (StringUtils.isEmpty(host) || StringUtils.isEmpty(port) || StringUtils.isEmpty(clusterName)) {
            throw new IllegalArgumentException("elasticsearch host|port|clusterName is not allow null");
        }

        String indexName = MapUtils.getString(configMap, "index", "");
        if (StringUtils.isEmpty(indexName))
            throw new IllegalArgumentException("elasticsearch indexName can not be null!");
        String typeName = MapUtils.getString(configMap, "type", indexName);

        sortField = MapUtils.getString(configMap, "sortField", "_uid");
        sortOrder = MapUtils.getString(configMap, "sortOrder", "asc");
        batchSize = MapUtils.getIntValue(configMap, "batchSize", 100);
        queryFieldName = MapUtils.getString(configMap, "queryFieldName", "");
        queryFieldValue = MapUtils.getString(configMap, "queryFieldValue", "");

        EsConfig esConfig = new EsConfig();
        esConfig.setPort(port);
        esConfig.setHost(host);
        esConfig.setClusterName(clusterName);
        elasticsearchQuery = new ElasticsearchQuery(esConfig, indexName, typeName);
    }

    @Override
    public SourceFunction<Object> getSourceFunction() {
        return this;
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        while (isRunning) {
            if (StringUtils.isEmpty(scrollId)) {
                scrollId = elasticsearchQuery.getScrollId(queryFieldName, queryFieldValue, batchSize, sortField, sortOrder, sortValue);
            }
            Map<String, Object> scrollMap = elasticsearchQuery.getScrollData(scrollId);
            scrollId = MapUtils.getString(scrollMap, "scrollId");

            List<Map<String, Object>> datas = (List<Map<String, Object>>) scrollMap.get("data");
            if (datas != null && datas.size() > 0) {
                Map<String, Object> latestData = datas.get(datas.size() - 1);
                sortValue = MapFieldUtil.getMapField(latestData, sortField);

                for (Map<String, Object> data : datas) {
                    ctx.collect(data);
                    totalCounter++;
                    sourceCounter.inc(1L);
                }
                datas.clear();
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void close() throws Exception {
        cancel();
        super.close();
    }

    //=============================================  指标  ==============================================================//
    private transient Counter sourceCounter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        sourceCounter = getRuntimeContext().getMetricGroup().counter("elasticsearch-source-counter");
    }

    //=============================================  备份检查点  ==============================================================//
    private volatile boolean isRunning = true;

    private String scrollId = null;
    private Object sortValue = null;
    private transient ListState<Object> timeCountState;

    private long totalCounter = 0;
    private transient ListState<Long> totalCounterState;

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        this.timeCountState.clear();
        this.timeCountState.add(sortValue);

        this.totalCounterState.clear();
        this.timeCountState.add(totalCounter);

        log.info("current scrollId:" + scrollId + ",sortValue:" + sortValue + ",totalCounter:" + totalCounter);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.timeCountState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("timeCountState", Object.class));
        this.totalCounterState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("totalCounterState", Long.class));
        if (context.isRestored()) {
            for (Object value : this.timeCountState.get()) {
                this.sortValue = value;
            }
            for (Long value : this.totalCounterState.get()) {
                this.totalCounter = value;
            }
            log.info("restored sortValue:" + sortValue + ",totalCounter" + totalCounter);
        } else {
            log.info("No restore state for ElasticsearchSource.");
        }
    }
}
