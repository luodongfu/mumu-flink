package com.lovecws.mumu.flink.streaming.duplicator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.Serializable;

/**
 * @program: act-able
 * @description: 数据备份抽象类，对源数据进行备份
 * @author: 甘亮
 * @create: 2019-06-05 09:18
 **/
public interface BaseDuplicator extends Serializable {

    public FilterFunction<Object> getDuplicatorFunction();
}
