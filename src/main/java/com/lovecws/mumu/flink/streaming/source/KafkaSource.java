package com.lovecws.mumu.flink.streaming.source;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.util.MapFieldUtil;
import com.lovecws.mumu.flink.streaming.common.serialization.ObjectDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @program: trunk
 * @description: kafka数据来源
 * @author: 甘亮
 * @create: 2019-11-01 13:29
 **/
@Slf4j
@ModularAnnotation(type = "source", name = "kafka")
public class KafkaSource implements BaseSource {

    private Properties props = new Properties();
    private String topic;
    private String offsetCommitMode;

    //偏移量模式 earliest_offset group_offset latest_offset specific_offsets
    private String startupMode;
    //EARLIEST LATEST
    private String autoOffsetReset;
    private long offset;
    private int partition;
    private String kafkaVersion;

    /**
     * kafka消费者
     */
    public KafkaSource(Map<String, Object> configMap) {
        if (configMap == null) throw new IllegalArgumentException("kafka config map not allow null");
        String brokers = MapUtils.getString(configMap, "brokers", MapFieldUtil.getMapField(configMap, "defaults.kafka.brokers").toString());
        this.topic = MapUtils.getString(configMap, "topic", "").toString();
        String groupid = MapUtils.getString(configMap, "groupid", "default-group").toString();

        //偏移量提交方式 disabled 、on_checkpoints 、 kafka_periodic
        this.offsetCommitMode = MapUtils.getString(configMap, "offsetCommitMode", "on_checkpoints").toString();

        //启动模式 group_offset、earliest_offset 、latest_offset、specific_offsets
        this.startupMode = MapUtils.getString(configMap, "startupMode", "group_offset").toString();
        this.autoOffsetReset = MapUtils.getString(configMap, "autoOffsetReset", "earliest").toString();

        String autocommit = MapUtils.getString(configMap, "autocommit", "true").toString();
        //如果设置为自动提交 则自动提交的频率

        int autoCommitIntervalMs = MapUtils.getIntValue(configMap, "autoCommitIntervalMs", 1000);
        this.offset = MapUtils.getIntValue(configMap, "offset", -1);
        this.partition = MapUtils.getIntValue(configMap, "partition", -1);
        //sesson的超时时间 当未自动提交代码的时候，如果设置的太小 可能造成数据提交失败
        int heartbeatIntervalMs = MapUtils.getIntValue(configMap, "heartbeatIntervalMs", 5000);
        int sessionTimeoutMs = MapUtils.getIntValue(configMap, "sessionTimeoutMs", 30000);
        int requestTimeoutMs = MapUtils.getIntValue(configMap, "requestTimeoutMs", 40000);
        if (sessionTimeoutMs > requestTimeoutMs) requestTimeoutMs = sessionTimeoutMs + 10000;
        //最大poll数据量
        int maxPollRecords = MapUtils.getIntValue(configMap, "maxPollRecords", 1000);
        //当消费者组没有offset的时候默认从那里开始拉取数据 earliest  latest seek
        //如果消费者组存在offset的时候 可以手动指定偏移量
        int maxPartitionFetchBytes = MapUtils.getIntValue(configMap, "maxPartitionFetchBytes", 1024 * 1024);
        int fetchMinBytes = MapUtils.getIntValue(configMap, "fetchMinBytes", 1);

        kafkaVersion = MapUtils.getString(configMap, "kafkaVersion", "v10");

        if (StringUtils.isEmpty(brokers) || StringUtils.isEmpty(topic))
            throw new IllegalArgumentException("kafka config brokers or topic not allow null");

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupid);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autocommit);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitIntervalMs);

        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        //props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        //kafka心跳时间
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);

        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
    }

    public SourceFunction<Object> getSourceFunction() {
        try {
            FlinkKafkaConsumerBase<Object> kafkaConsumer = null;
            if ("v9".equalsIgnoreCase(kafkaVersion)) {
                kafkaConsumer = new FlinkKafkaConsumer09<Object>(this.topic, new ObjectDeserializationSchema(), props);
            } else if ("v10".equalsIgnoreCase(kafkaVersion)) {
                kafkaConsumer = new FlinkKafkaConsumer010<Object>(this.topic, new ObjectDeserializationSchema(), props);
            } else {
                throw new RuntimeException();
            }
            if ("group_offset".equalsIgnoreCase(startupMode)) {
                kafkaConsumer.setStartFromGroupOffsets();
            } else if ("earliest_offset".equalsIgnoreCase(startupMode)) {
                kafkaConsumer.setStartFromEarliest();
            } else if ("latest_offset".equalsIgnoreCase(startupMode)) {
                kafkaConsumer.setStartFromLatest();
            } else if ("specific_offsets".equalsIgnoreCase(startupMode)) {
                Map<KafkaTopicPartition, Long> specificStartupOffsets = new HashMap<>();
                KafkaTopicPartition kafkaTopicPartition = new KafkaTopicPartition(this.topic, this.partition);
                specificStartupOffsets.put(kafkaTopicPartition, offset);
                kafkaConsumer.setStartFromSpecificOffsets(specificStartupOffsets);
            }
            //偏移量提交方式
            if ("on_checkpoints".equalsIgnoreCase(offsetCommitMode)) {
                kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
            } else if ("kafka_periodic".equalsIgnoreCase(offsetCommitMode)) {
            } else if ("disabled".equalsIgnoreCase(offsetCommitMode)) {
            }
            return kafkaConsumer;
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException(e);
        }
    }
}
