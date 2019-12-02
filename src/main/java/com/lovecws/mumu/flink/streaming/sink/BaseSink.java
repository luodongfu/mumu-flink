package com.lovecws.mumu.flink.streaming.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.Serializable;

/**
 * @program: trunk
 * @description: 基类sink
 * @author: 甘亮
 * @create: 2019-11-01 14:03
 **/
public interface BaseSink extends Serializable {

    public SinkFunction<Object> getSinkFunction();
}
