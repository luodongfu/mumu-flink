package com.lovecws.mumu.flink.streaming.source;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.util.HttpClientUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @program: act-able
 * @description: 从http去读消息，支持横向拓展
 * @author: 甘亮
 * @create: 2019-07-30 17:51
 **/
@Slf4j
@ModularAnnotation(type = "source", name = "http")
public class HttpDataSource extends AbstractRichFunction implements BaseSource, SourceFunction<Object> {

    private String requestUrl;
    private String requestMethod;
    private Map<String, Object> requestHeaders;
    private Map<String, Object> requestParams;

    /**
     * 初始化参数获取
     */
    public HttpDataSource(Map<String, Object> configMap) {
        requestUrl = MapUtils.getString(configMap, "url", "").toString();
        requestMethod = MapUtils.getString(configMap, "method", "get").toString();
        requestHeaders = MapUtils.getMap(configMap, "headers", new HashMap<String, Object>());
        requestParams = MapUtils.getMap(configMap, "params", new HashMap<String, Object>());
    }

    @Override
    public SourceFunction<Object> getSourceFunction() {
        return this;
    }

    @Override
    public void run(SourceContext<Object> ctx) throws Exception {
        while (this.isRunning) {
            List<Object> datas = new ArrayList<>();
            if (requestMethod.toUpperCase().equals("GET")) {
                datas.add(HttpClientUtil.get(requestUrl, requestHeaders));
            } else if (requestMethod.toUpperCase().equals("POST")) {
                datas.add(HttpClientUtil.post(requestUrl, requestParams, requestHeaders));
            }
            for (Object object : datas) {
                ctx.collect(object);
                sourceCounter.inc(1L);
            }
            break;
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }

    @Override
    public void close() throws Exception {
        this.cancel();
        super.close();
    }

    //=============================================  指标  ==============================================================//
    private transient Counter sourceCounter;
    private volatile boolean isRunning = true;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        sourceCounter = getRuntimeContext().getMetricGroup().counter(this.getClass().getSimpleName() + "-source-counter");
    }

}
