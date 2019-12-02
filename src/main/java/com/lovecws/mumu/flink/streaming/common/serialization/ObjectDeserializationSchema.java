package com.lovecws.mumu.flink.streaming.common.serialization;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;

import java.io.IOException;

public class ObjectDeserializationSchema extends AbstractDeserializationSchema<Object> {
    @Override
    public Object deserialize(byte[] bytes) throws IOException {
        return bytes;
    }
}