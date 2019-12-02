package com.lovecws.mumu.flink.streaming.backup;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.util.DateUtils;
import com.lovecws.mumu.flink.streaming.common.encoder.BatchStringEncoder;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.Map;

/**
 * @program: trunk
 * @description: 源数据备份
 * @author: 甘亮
 * @create: 2019-11-01 14:01
 **/
@Slf4j
@ModularAnnotation(type = "backup", name = "localfile")
public class LocalFileBackup implements BaseBackup {

    private String path;
    private long bucketInterval;
    private String bucketFormat;

    private long rollingPartSize;
    private long rolloverInterval;
    private long rollingInactivityInterval;

    public LocalFileBackup(Map<String, Object> configMap) {
        this.path = configMap.getOrDefault("path", "/tmp").toString();
        this.bucketInterval = DateUtils.getExpireTime(configMap.getOrDefault("bucketInterval", "60s").toString()) * 1000L;
        this.bucketFormat = configMap.getOrDefault("bucketFormat", "yyyyMMddHH").toString();

        this.rollingPartSize = Long.parseLong(configMap.getOrDefault("rollingPartSize", 1024L * 1024L * 128L).toString());
        this.rolloverInterval = DateUtils.getExpireTime(configMap.getOrDefault("rolloverInterval", "60s").toString()) * 1000L;
        this.rollingInactivityInterval = DateUtils.getExpireTime(configMap.getOrDefault("rollingInactivityInterval", "60s").toString()) * 1000L;
    }

    public SinkFunction<Object> getBackupFunction() {
        StreamingFileSink.RowFormatBuilder<Object, String> builder = StreamingFileSink.forRowFormat(new Path(path), new BatchStringEncoder());
        builder.withBucketAssigner(new DateTimeBucketAssigner<Object>(bucketFormat));
        builder.withBucketCheckInterval(bucketInterval);

        DefaultRollingPolicy<Object, String> rollingPolicy = DefaultRollingPolicy.create()
                .withMaxPartSize(rollingPartSize)
                .withRolloverInterval(rolloverInterval)
                .withInactivityInterval(rollingInactivityInterval)
                .build();
        builder.withRollingPolicy(rollingPolicy);
        return builder.build();
    }
}
