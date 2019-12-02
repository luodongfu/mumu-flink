package com.lovecws.mumu.flink.streaming.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.Serializable;

/**
 * @program: trunk
 * @description: 数据源接口
 * @author: 甘亮
 * @create: 2019-11-01 14:59
 **/
public interface BaseSource extends Serializable {
    public SourceFunction<Object> getSourceFunction();
}
