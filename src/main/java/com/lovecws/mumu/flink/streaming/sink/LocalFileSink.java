package com.lovecws.mumu.flink.streaming.sink;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.model.atd.AtdEventModel;
import com.lovecws.mumu.flink.common.util.AvroUtil;
import com.lovecws.mumu.flink.common.util.DateUtils;
import com.lovecws.mumu.flink.common.util.ParquetUtil;
import com.lovecws.mumu.flink.streaming.common.encoder.BatchStringEncoder;
import com.lovecws.mumu.flink.streaming.common.encoder.CsvEncoder;
import com.lovecws.mumu.flink.streaming.common.factory.AvroFactory;
import com.lovecws.mumu.flink.streaming.common.factory.ParquetFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @program: trunk
 * @description: 文件存储
 * @author: 甘亮
 * @create: 2019-11-01 14:01
 **/
@Slf4j
@ModularAnnotation(type = "sink", name = "localfile")
public class LocalFileSink extends RichSinkFunction<Object> implements BaseSink {

    public String path;
    public String fileType;//文件类型 txt json

    private String[] fields;//写入文件的全部字段,如果fields为空，那么就从对象模型获取字段列表
    private String[] uploadFields;//需要写入文件的字段列表

    private long bucketInterval;
    private String bucketFormat;

    private long rollingPartSize;
    private long rolloverInterval;
    private long rollingInactivityInterval;

    private String[] getUploadFields(String[] fields, String[] emptyFields) {
        List<String> uploadFields = new ArrayList<>();
        for (String fd : fields) {
            boolean isUpload = true;
            for (String ef : emptyFields) {
                if (ef.equalsIgnoreCase(fd)) {
                    isUpload = false;
                    break;
                }
            }
            if (isUpload) uploadFields.add(fd);
        }
        return StringUtils.split(StringUtils.join(uploadFields, ","), ",");
    }

    public LocalFileSink(Map<String, Object> configMap) {
        path = MapUtils.getString(configMap, "path", "/tmp/industrystreaming/localfile").toString();
        fileType = MapUtils.getString(configMap, "fileType", "txt").toString();

        fields = MapUtils.getString(configMap, "fileds", "").split(",");
        uploadFields = MapUtils.getString(configMap, "uploadFields", "").split(",");
        if (StringUtils.isBlank(uploadFields[0])) {
            uploadFields = getUploadFields(fields, MapUtils.getString(configMap, "emptyFileds", "").split(","));
        }

        bucketInterval = DateUtils.getExpireTime(MapUtils.getString(configMap, "bucketInterval", "60s").toString()) * 1000L;
        bucketFormat = MapUtils.getString(configMap, "bucketFormat", "yyyyMMddHH").toString();

        rollingPartSize = MapUtils.getLongValue(configMap, "rollingPartSize", 1024L * 1024L * 128L);
        rolloverInterval = DateUtils.getExpireTime(MapUtils.getString(configMap, "rolloverInterval", "60s").toString()) * 1000L;
        rollingInactivityInterval = DateUtils.getExpireTime(MapUtils.getString(configMap, "rollingInactivityInterval", "60s").toString()) * 1000L;
    }

    private StreamingFileSink<Object> forBulkFormat(BulkWriter.Factory<Object> factory) {
        StreamingFileSink.BulkFormatBuilder<Object, String> bulkFormatBuilder = StreamingFileSink.forBulkFormat(new Path(path), factory);
        bulkFormatBuilder = bulkFormatBuilder.withBucketAssigner(new DateTimeBucketAssigner<Object>(bucketFormat));
        bulkFormatBuilder = bulkFormatBuilder.withBucketCheckInterval(bucketInterval);
        return bulkFormatBuilder.build();
    }

    private StreamingFileSink<Object> forRowFormat(Encoder<Object> encoder) {
        StreamingFileSink.RowFormatBuilder<Object, String> builder = StreamingFileSink.forRowFormat(new Path(path), encoder);
        DefaultRollingPolicy<Object, String> rollingPolicy = DefaultRollingPolicy.create()
                .withMaxPartSize(rollingPartSize)
                .withRolloverInterval(rolloverInterval)
                .withInactivityInterval(rollingInactivityInterval)
                .build();
        builder = builder.withBucketAssignerAndPolicy(new DateTimeBucketAssigner<Object>(bucketFormat), rollingPolicy);
        builder = builder.withBucketCheckInterval(bucketInterval);
        return builder.build();
    }

    public SinkFunction<Object> getSinkFunction() {
        BulkWriter.Factory<Object> factory = null;
        Encoder<Object> encoder = null;
        if ("avro".equalsIgnoreCase(fileType)) {
            factory = new AvroFactory<Object>(AvroUtil.getSchema(AtdEventModel.class));
        } else if ("parquet".equalsIgnoreCase(fileType)) {
            factory = new ParquetFactory<>(ParquetUtil.getMessageType(AtdEventModel.class));
        } else if ("csv".equalsIgnoreCase(fileType) || "txt".equalsIgnoreCase(fileType)) {
            encoder = new CsvEncoder(this.getClass().getSimpleName(), path, fields, uploadFields);
        } else {
            encoder = new BatchStringEncoder();
        }
        //bulk
        if (factory != null) return forBulkFormat(factory);
        //row行格式存储
        return forRowFormat(encoder);
    }
}
