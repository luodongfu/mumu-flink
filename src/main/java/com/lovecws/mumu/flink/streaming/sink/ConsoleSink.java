package com.lovecws.mumu.flink.streaming.sink;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Map;

/**
 * @program: trunk
 * @description: 控制台打印
 * @author: 甘亮
 * @create: 2019-11-01 15:39
 **/
@Slf4j
@ModularAnnotation(type = "sink", name = "console")
public class ConsoleSink implements BaseSink {
    public ConsoleSink(Map<String, Object> configMap) {
    }

    @Override
    public SinkFunction<Object> getSinkFunction() {
        return new PrintSinkFunction();
    }
}
