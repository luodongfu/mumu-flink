package com.lovecws.mumu.flink.streaming.task.builtin;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.streaming.task.AbstractStreamingTask;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * @program: act-able
 * @description: 内置的流处理程序
 * @author: 甘亮
 * @create: 2019-11-20 23:54
 **/
@Slf4j
@ModularAnnotation(type = "task", name = "buillin")
public class BuillinStreamingTask extends AbstractStreamingTask {
    public BuillinStreamingTask(Map<String, Object> configMap) {
        super(configMap);
    }

    @Override
    public Object parseData(Object data) {
        return data;
    }

    @Override
    public Boolean filterData(Object data) {
        return true;
    }

    @Override
    public Object fillData(Object data) {
        return data;
    }
}
