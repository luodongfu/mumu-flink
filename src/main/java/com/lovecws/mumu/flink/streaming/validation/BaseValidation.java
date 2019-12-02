package com.lovecws.mumu.flink.streaming.validation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.Serializable;

/**
 * @program: act-able
 * @description: 数据校验接口
 * @author: 甘亮
 * @create: 2019-06-05 09:18
 **/
public interface BaseValidation extends Serializable {

    public FilterFunction<Object> getValidationFunction();
}
