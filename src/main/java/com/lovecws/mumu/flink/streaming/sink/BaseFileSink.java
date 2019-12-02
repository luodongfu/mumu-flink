package com.lovecws.mumu.flink.streaming.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.util.*;

/**
 * @program: act-able
 * @description: 文件存储，依赖checkpoints机制。
 * @author: 甘亮
 * @create: 2019-06-11 13:11
 **/
@Slf4j
@Deprecated
@ModularAnnotation(type = "sink", name = "basefile")
public class BaseFileSink extends RichSinkFunction<Object> implements BaseSink, CheckpointedFunction {

    private static final String TIMECOUNT_KEY = "BaseFileSinkTimeCounter";

    //文件配置信息
    //文件类型
    public String fileType;
    //文件根目录
    public String path;

    //csv文件格式
    public List<String> csvHeaders = new ArrayList<>();
    public String csvSeparator;
    public boolean csvHeader;

    //txt文件格式
    public String[] fields;
    //指定为空的field，可以此字段必须必为空
    public String[] emptyFields;
    public String[] uploadFields;

    public int lineCount;//收集生成的文件行数，-1 则收集所有的文件 10000每个收集的文件为1000条数据
    public long timeCount;//当前时间-文件最后修改时间多久才能收集，防止收集到正在写入的文件

    private String codec;

    //checkpoint检查点
    private transient ListState<Object> listState;
    private transient BroadcastState<String, Long> timeCountState;
    private List<Object> cacheDatas;

    //过滤规则,过滤不需要的字段
    private String filter;

    public BaseFileSink(Map<String, Object> configMap) {
        this.path = MapUtils.getString(configMap, "path", "").replace("\\", "/");
        if (StringUtils.isEmpty(path)) path = "/tmp/";
        if (!this.path.endsWith("/")) this.path = this.path + "/";
        fileType = MapUtils.getString(configMap, "fileType", "json").toLowerCase();

        //csv文件分隔符
        csvSeparator = MapUtils.getString(configMap, "csvSeparator", "|");
        if (StringUtils.isEmpty(csvSeparator)) csvSeparator = "|";
        csvHeader = MapUtils.getBooleanValue(configMap, "csvHeader", true);

        //txt数据分割
        fields = MapUtils.getString(configMap, "fileds", "").split(",");
        emptyFields = MapUtils.getString(configMap, "emptyFileds", "").split(",");
        uploadFields = MapUtils.getString(configMap, "uploadFields", "").split(",");

        //文件收集策略
        lineCount = Integer.parseInt(configMap.getOrDefault("lineCount", "10000").toString());
        timeCount = DateUtils.getExpireTime(MapUtils.getString(configMap, "timeCount", "5m")) * 1000L;

        codec = MapUtils.getString(configMap, "codec", "snappy");
        cacheDatas = new ArrayList<>();

        filter = MapUtils.getString(configMap, "filter", "").toString();
    }

    @Override
    public SinkFunction<Object> getSinkFunction() {
        return this;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        Long currentTime = timeCountState.get(TIMECOUNT_KEY);
        List<Object> datas = new ArrayList<>(cacheDatas);
        //触发文件保存，将内存中的数据写入到文件中
        if (datas.size() >= lineCount || System.currentTimeMillis() - currentTime > timeCount) {
            writeFile(datas, fileType);
            //删除已经写入到文件的数据
            cacheDatas.removeAll(datas);
            listState.clear();
            timeCountState.put(TIMECOUNT_KEY, System.currentTimeMillis());
        }
        listState.addAll(cacheDatas);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Object> stateDescriptor = new ListStateDescriptor<>("BaseFileSink-liststate", Object.class);
        listState = context.getOperatorStateStore().getListState(stateDescriptor);

        MapStateDescriptor<String, Long> valueStateDescriptor = new MapStateDescriptor<String, Long>("BaseFileSink-timercount", String.class, Long.class);
        //valueStateDescriptor.setQueryable("base-file-timercount");
        timeCountState = context.getOperatorStateStore().getBroadcastState(valueStateDescriptor);
        //从检查点恢复数据
        if (context.isRestored()) {
            for (Object object : this.listState.get()) {
                cacheDatas.add(object);
            }
        } else {
            timeCountState.put(TIMECOUNT_KEY, System.currentTimeMillis());
        }
    }

    @Override
    public void invoke(Object value, Context context) throws Exception {
        List<Object> datas = new ArrayList<>();
        if (value instanceof List) {
            datas.addAll((List) value);
        } else {
            datas.add(value);
        }
        datas.removeIf(obj -> StorageFilterUtil.filter(obj, filter));
        cacheDatas.addAll(datas);
        if (datas.size() > 1)
            log.info("baseFileSinkInvoke:{path:" + path + ",fileType:" + fileType + ",codec:" + codec + ",lineCount:" + lineCount + ",timeCount:" + timeCount + ",size:" + datas.size() + "}");
    }

    /**
     * 将数据文件写入到文件中
     *
     * @param datas    数据集合
     * @param fileType 文件类型
     */
    private void writeFile(List<Object> datas, String fileType) throws IOException {
        if (datas.size() == 0) return;
        String filePath = path + DateUtils.getCurrentDayStr() + "/";
        FileUtils.forceMkdir(new File(filePath));
        String uuid = UUID.randomUUID().toString().replaceAll("-", "").substring(0, 6);
        String outFile = filePath + DateUtils.formatDate(new Date(), "yyyyMMddHHmmssSSS") + uuid + "." + fileType.toLowerCase();

        switch (fileType) {
            case "csv":
                writeCsvFile(datas, outFile);
                break;
            case "txt":
                writeTxtFile(datas, outFile);
                break;
            case "json":
                writeJsonFile(datas, outFile);
                break;
            case "avro":
                writeAvroFile(datas, outFile);
                break;
            case "parquet":
                writeParquetFile(datas, outFile);
                break;
            default:
                throw new IllegalArgumentException();
        }
        log.info("write " + fileType + " file:" + outFile + " datasize:" + datas.size());
    }

    /**
     * 写入csv文件
     *
     * @param datas   数据集
     * @param outFile 输出文件
     */
    private void writeCsvFile(List<Object> datas, String outFile) {
        OutputStream outputStream = null;
        try {
            outputStream = new FileOutputStream(outFile);
            for (Object data : datas) {
                Map<String, Object> map = JSON.parseObject(JSON.toJSONString(data), Map.class);
                if (csvHeaders.size() == 0 && csvHeader) {
                    List<Map<String, Object>> headerMaps = MapFieldUtil.getFlatMapKeys(map);
                    headerMaps.forEach(headerMap -> csvHeaders.add(headerMap.get("name").toString()));
                    outputStream.write(StringUtils.join(csvHeaders, csvSeparator).getBytes());
                    outputStream.write("\n".getBytes());
                }
                List<Object> values = MapFieldUtil.getFlatMapValues(map, csvHeaders);

                //将数据格式化 日期 换行符
                List<Object> objects = new ArrayList<>();
                values.forEach(object -> {
                    if (object == null) object = "";
                    if (object instanceof Date) {
                        object = DateUtils.formatDate((Date) object, "yyyy-MM-dd HH:mm:ss");
                    } else if (object instanceof String) {
                        String cvalue = String.valueOf(object);
                        cvalue = cvalue.replaceAll(",", "").replaceAll("\"", "").replaceAll("'", "");
                        if (cvalue.contains("\n")) cvalue = URLEncoder.encode(cvalue);
                        if (cvalue.contains(csvSeparator)) cvalue = cvalue.replaceAll(csvSeparator, "");
                        object = cvalue;
                    }
                    objects.add(object);
                });
                outputStream.write(StringUtils.join(objects, csvSeparator).getBytes());
                outputStream.write("\n".getBytes());
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage());
        } finally {
            IOUtils.closeQuietly(outputStream);
        }
    }

    /**
     * 写入json文件
     *
     * @param datas   数据集
     * @param outFile 输出文件
     */
    private void writeJsonFile(List<Object> datas, String outFile) {
        OutputStream outputStream = null;
        try {
            outputStream = new FileOutputStream(outFile);
            for (Object data : datas) {
                outputStream.write(JSON.toJSONStringWithDateFormat(data, "yyyy-MM-dd HH:mm:ss", SerializerFeature.WriteMapNullValue).getBytes());
                outputStream.write("\n".getBytes());
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage());
        } finally {
            IOUtils.closeQuietly(outputStream);
        }
    }

    /**
     * 写入txt文件
     *
     * @param datas   数据集
     * @param outFile 输出文件
     */
    private void writeTxtFile(List<Object> datas, String outFile) {
        OutputStream outputStream = null;
        try {
            outputStream = new FileOutputStream(outFile);
            for (Object data : datas) {
                outputStream.write(TxtUtils.readLine(data, fields, uploadFields).getBytes());
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage());
        } finally {
            IOUtils.closeQuietly(outputStream);
        }
    }

    /**
     * 写入avro文件
     *
     * @param datas   数据集
     * @param outFile 输出文件
     */
    private void writeAvroFile(List<Object> datas, String outFile) {
        DataFileWriter<Object> avroWriter = null;
        Schema schema = null;
        try {
            for (Object data : datas) {
                if (avroWriter == null) {
                    schema = AvroUtil.getSchema(data);
                    DataFileWriter<Object> dataFileWriter = new DataFileWriter<Object>(new GenericDatumWriter<>(schema));
                    dataFileWriter.setCodec(getAvroCodecFactory(codec));
                    avroWriter = dataFileWriter.create(schema, new File(outFile));
                }
                avroWriter.append(AvroUtil.getRecord(data, schema));
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage());
            ex.printStackTrace();
        } finally {
            IOUtils.closeQuietly(avroWriter);
        }
    }

    /**
     * 写入parquet文件
     *
     * @param datas   数据集
     * @param outFile 输出文件
     */
    private void writeParquetFile(List<Object> datas, String outFile) {
        ParquetWriter<Group> parquetWriter = null;
        MessageType messageType = null;
        try {
            for (Object data : datas) {
                if (parquetWriter == null) {
                    messageType = ParquetUtil.getMessageType(data);
                    Configuration conf = new Configuration();
                    GroupWriteSupport.setSchema(messageType, conf);
                    parquetWriter = new ParquetWriter<Group>(new Path(outFile), new GroupWriteSupport(),
                            getParquetCompressionCodecName(codec),
                            ParquetWriter.DEFAULT_BLOCK_SIZE,
                            ParquetWriter.DEFAULT_PAGE_SIZE,
                            ParquetWriter.DEFAULT_PAGE_SIZE,
                            ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                            ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                            ParquetProperties.WriterVersion.PARQUET_1_0, conf);
                }
                parquetWriter.write(ParquetUtil.getGroup(messageType, data));
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage());
        } finally {
            IOUtils.closeQuietly(parquetWriter);
        }
    }

    /**
     * 获取codec工厂
     *
     * @param codec codec
     * @return
     */
    private CodecFactory getAvroCodecFactory(String codec) {
        CodecFactory codecFactory = null;
        if ("snappy".equalsIgnoreCase(codec)) {
            codecFactory = CodecFactory.snappyCodec();
        } else if ("null".equalsIgnoreCase(codec)) {
            codecFactory = CodecFactory.nullCodec();
        } else if ("deflate".equalsIgnoreCase(codec)) {
            codecFactory = CodecFactory.deflateCodec(1);
        } else if ("bzip2".equalsIgnoreCase(codec)) {
            codecFactory = CodecFactory.bzip2Codec();
        } else {
            throw new IllegalArgumentException();
        }
        return codecFactory;
    }

    public CompressionCodecName getParquetCompressionCodecName(String codec) {
        CompressionCodecName compressionCodecName = null;
        if ("snappy".equalsIgnoreCase(codec)) {
            compressionCodecName = CompressionCodecName.SNAPPY;
        } else if ("gzip".equalsIgnoreCase(codec)) {
            compressionCodecName = CompressionCodecName.GZIP;
        } else if ("uncompressed".equalsIgnoreCase(codec)) {
            compressionCodecName = CompressionCodecName.UNCOMPRESSED;
        } else if ("lzo".equalsIgnoreCase(codec)) {
            compressionCodecName = CompressionCodecName.LZO;
        } else {
            throw new IllegalArgumentException();
        }
        return compressionCodecName;
    }
}
