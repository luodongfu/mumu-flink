package com.lovecws.mumu.flink.streaming.backup;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.Serializable;

/**
 * @program: act-able
 * @description: 数据备份接口，对源数据进行备份
 * @author: 甘亮
 * @create: 2019-06-05 09:18
 **/
public interface BaseBackup extends Serializable {

    public SinkFunction<Object> getBackupFunction();
}
