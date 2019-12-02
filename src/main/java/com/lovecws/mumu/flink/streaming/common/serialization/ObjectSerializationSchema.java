package com.lovecws.mumu.flink.streaming.common.serialization;

import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * @program: trunk
 * @description: 序列化
 * @author: 甘亮
 * @create: 2019-11-04 16:26
 **/
public class ObjectSerializationSchema implements SerializationSchema {
    @Override
    public byte[] serialize(Object element) {
        return element.toString().getBytes();
    }
}
