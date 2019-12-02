package com.lovecws.mumu.flink.streaming.sink;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.util.MapFieldUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Map;
import java.util.Properties;

/**
 * @program: trunk
 * @description: kafka存储
 * @author: 甘亮
 * @create: 2019-11-01 13:47
 **/
@Slf4j
@ModularAnnotation(type = "sink", name = "kafka")
public class KafkaSink implements BaseSink {

    private Properties props = new Properties();
    private String topic;
    private String serializerSchema;

    public KafkaSink(Map<String, Object> configMap) {
        if (configMap == null) throw new IllegalArgumentException("kafka config map not allow null");
        String brokers = MapUtils.getString(configMap, "brokers", MapFieldUtil.getMapField(configMap, "defaults.kafka.brokers").toString());
        this.topic = MapUtils.getString(configMap, "topic", "").toString();

        //"all", "-1", "0", "1"
        int acks = MapUtils.getIntValue(configMap, "acks", 1);
        int retries = MapUtils.getIntValue(configMap, "retries", 3);

        //序列化
        this.serializerSchema = MapUtils.getString(configMap, "serializer", "org.apache.flink.streaming.util.serialization.SimpleStringSchema").toString();

        //压缩格式  none、gzip、snappy、lz4
        String compression_type = MapUtils.getString(configMap, "compression_type", "none").toString();

        //sesson的超时时间 当未自动提交代码的时候，如果设置的太小 可能造成数据提交失败
        int requestTimeoutMs = MapUtils.getIntValue(configMap, "requestTimeoutMs", 40000);

        if (StringUtils.isEmpty(topic))
            throw new IllegalArgumentException("kafka config brokers or topic not allow null");

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.RETRIES_CONFIG, retries);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression_type);
    }

    @Override
    public SinkFunction<Object> getSinkFunction() {
        try {
            Class serializationSchemaClass = Class.forName(this.serializerSchema);
            SerializationSchema serializationSchema = (SerializationSchema) ConstructorUtils.invokeConstructor(serializationSchemaClass);
            return new FlinkKafkaProducer09(this.topic, serializationSchema, props);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException(e);
        }
    }
}
