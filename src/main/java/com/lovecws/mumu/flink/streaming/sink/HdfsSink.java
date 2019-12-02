package com.lovecws.mumu.flink.streaming.sink;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.util.DateUtils;
import com.lovecws.mumu.flink.common.util.StorageFilterUtil;
import com.lovecws.mumu.flink.streaming.common.writer.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.hadoop.io.SequenceFile;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @program: act-able
 * @description: hdfs文件存储
 * @author: 甘亮
 * @create: 2019-06-11 13:11
 **/
@Slf4j
@ModularAnnotation(type = "sink", name = "hdfs")
public class HdfsSink extends BucketingSink<Object> implements BaseSink {

    private String path;
    //过滤数据
    private String filter;
    //文件类型
    private String fileType;
    //文件编码
    private String codec;

    //文件csv字段列表
    public String[] fields;
    //文件csv需要上报的字段值
    public String[] uploadFields;

    //bucket批量大小和时间
    private long batchSize;
    private long rolloverInterval;

    public HdfsSink(Map<String, Object> configMap) {
        super(MapUtils.getString(configMap, "path", "/tmp").replace("\\", "/"));
        path = MapUtils.getString(configMap, "path", "/tmp").replace("\\", "/");
        fileType = MapUtils.getString(configMap, "fileType", "json");
        codec = MapUtils.getString(configMap, "codec", "");

        filter = MapUtils.getString(configMap, "filter", "");

        //文件大小
        batchSize = MapUtils.getLongValue(configMap, "batchSize", 1024 * 1024 * 64);
        rolloverInterval = DateUtils.getExpireTime(MapUtils.getString(configMap, "rolloverInterval", "1m"));

        //txt数据分割
        fields = MapUtils.getString(configMap, "fileds", "").split(",");
        uploadFields = MapUtils.getString(configMap, "uploadFields", "").split(",");
    }

    @Override
    public void invoke(Object value) throws Exception {
        List<Object> datas = new ArrayList<>();
        if (value instanceof List) {
            datas.addAll((List) value);
        } else {
            datas.add(value);
        }
        datas.removeIf(obj -> StorageFilterUtil.filter(obj, filter));
        super.invoke(datas);
        if (datas.size() > 1)
            log.info("hdfsSinkInvoke:{path:" + path + ",fileType:" + fileType + ",codec:" + codec + ",batchSize:" + batchSize + ",rolloverInterval:" + rolloverInterval + ",size:" + datas.size() + "}");
    }

    @Override
    public SinkFunction<Object> getSinkFunction() {
        setBucketer(new DateTimeBucketer<>("yyyyMMddHH", ZoneId.of("Asia/Shanghai")));

        if ("sequencefile".equalsIgnoreCase(fileType)) {
            setWriter(new SequencefileWriter<>(codec, SequenceFile.CompressionType.NONE));
        } else if ("avro".equalsIgnoreCase(fileType)) {
            setWriter(new AvroFileWriter<>(codec));
        } else if ("parquet".equalsIgnoreCase(fileType)) {
            setWriter(new ParquetFileWriter<>(codec));
        } else if ("json".equalsIgnoreCase(fileType)) {
            setWriter(new JsonFileWriter<>());
        } else if ("csv".equalsIgnoreCase(fileType) || "txt".equalsIgnoreCase(fileType)) {
            setWriter(new CsvFileWriter<>(fields, uploadFields));
        } else {
            setWriter(new StringWriter<>());
        }

        // 下述两种条件满足其一时，创建新的块文件
        setBatchSize(batchSize);
        // 条件2.设置时间间隔20min
        setBatchRolloverInterval(rolloverInterval);
        // 设置块文件前缀
        setPendingPrefix("");
        // 设置块文件后缀
        setPendingSuffix("");
        // 设置运行中的文件前缀
        setInProgressPrefix(".");

        //设置part文件后缀
        setPartSuffix("." + fileType);

        return this;
    }


}
