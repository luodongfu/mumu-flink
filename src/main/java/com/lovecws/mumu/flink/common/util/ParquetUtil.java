package com.lovecws.mumu.flink.common.util;

import com.alibaba.fastjson.JSON;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.parquet.CorruptStatistics;
import org.apache.parquet.Log;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.*;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * @program: act-able
 * @description: parquet工具类，提供根据数据模型获取parquet的schema方法、读parquet文件、写parquet文件、合并同类型parquet文件
 * @author: 甘亮
 * @create: 2019-06-11 18:22
 **/
public class ParquetUtil {

    private static final Logger log = Logger.getLogger(ParquetUtil.class);

    //去除parquet日志信息
    static {
        try {
            //去除info級別日志
            Field field = FieldUtils.getField(Log.class, "INFO", true);
            FieldUtils.removeFinalModifier(field, true);
            Object object = null;
            FieldUtils.writeField(field, object, false, true);

            //关闭日志
            offParquetLogger(ParquetFileReader.class, "LOG", true);
            offParquetLogger(ParquetFileWriter.class, "LOG", true);
            offParquetLogger(ParquetRecordReader.class, "LOG", true);

            offParquetLogger(CorruptStatistics.class, "LOG", true);

            offParquetLogger(Class.forName("org.apache.parquet.hadoop.InternalParquetRecordReader"), "LOG", true);
            offParquetLogger(Class.forName("org.apache.parquet.hadoop.InternalParquetRecordWriter"), "LOG", true);
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        }
    }

    private static void offParquetLogger(Class clazz, String logField, boolean isStatic) {
        try {
            Field parquetLoggerField = FieldUtils.getField(clazz, logField, true);
            if (isStatic) {
                FieldUtils.removeFinalModifier(parquetLoggerField, true);
            }

            Field loggerField = FieldUtils.getField(Log.class, "logger", true);

            Log LOG = Log.getLog(clazz);
            java.util.logging.Logger logger = java.util.logging.Logger.getLogger(clazz.getName());
            logger.setLevel(Level.OFF);
            FieldUtils.writeField(loggerField, LOG, logger, true);

            Object readerLoggerObject = null;
            FieldUtils.writeField(parquetLoggerField, readerLoggerObject, LOG, true);
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage());
        }
    }

    /**
     * 获取parquet的schema
     *
     * @param object 对象
     *               每一个字段有三个属性：重复数、数据类型和字段名，重复数可以是以下三种：required(出现1次), repeated(出现0次或多次) ,optional(出现0次或1次)
     *               每一个字段的数据类型可以分成两种：group(复杂类型) primitive(基本类型)
     *               数据类型有 INT64, INT32, BOOLEAN, BINARY, FLOAT, DOUBLE, INT96, FIXED_LEN_BYTE_ARRAY
     * @return
     */
    @Deprecated
    public static MessageType parseMessageType(Object object) {
        List<Map<String, String>> tableFields = TableUtil.getTableFields(object);
        StringBuffer messageTypeBuffer = new StringBuffer("message " + object.getClass().getSimpleName() + " {");
        tableFields.forEach(flatMap -> {
            String name = flatMap.get("name");
            String type = flatMap.get("type");

            String parquetType = "";
            switch (type) {
                case "int":
                    parquetType = "optional INT32 " + name + ";";
                    break;
                case "double":
                    parquetType = "optional DOUBLE " + name + ";";
                    break;
                case "bigint":
                case "long":
                    parquetType = "optional INT64 " + name + ";";
                    break;
                default:
                    parquetType = "optional BINARY " + name + " (UTF8);";
            }
            messageTypeBuffer.append(parquetType);
        });
        messageTypeBuffer.append("}");

        return MessageTypeParser.parseMessageType(messageTypeBuffer.toString());
    }

    /**
     * 根据对象 找到pqrquet的schema
     *
     * @param object 对象
     * @return parquet的schema信息
     */
    public static MessageType getMessageType(Object object) {
        List<Type> fields = new ArrayList<>();

        List<Map<String, String>> tableFields = TableUtil.getTableFields(object);
        tableFields.forEach(flatMap -> {
            String name = flatMap.get("name");
            String type = flatMap.get("type").toLowerCase();

            PrimitiveType.PrimitiveTypeName primitiveTypeName = null;
            OriginalType originalType = null;
            switch (type) {
                case "int":
                    primitiveTypeName = PrimitiveType.PrimitiveTypeName.INT32;
                    originalType = OriginalType.INT_32;
                    break;
                case "bigint":
                case "long":
                    primitiveTypeName = PrimitiveType.PrimitiveTypeName.INT64;
                    originalType = OriginalType.INT_64;
                    break;
                case "float":
                    primitiveTypeName = PrimitiveType.PrimitiveTypeName.FLOAT;
                    break;
                case "double":
                    primitiveTypeName = PrimitiveType.PrimitiveTypeName.DOUBLE;
                    break;
                case "boolean":
                    primitiveTypeName = PrimitiveType.PrimitiveTypeName.BOOLEAN;
                    break;
                case "date":
                case "timestamp":
                    primitiveTypeName = PrimitiveType.PrimitiveTypeName.INT96;
                    break;
                default:
                    primitiveTypeName = PrimitiveType.PrimitiveTypeName.BINARY;
                    originalType = OriginalType.UTF8;
            }
            fields.add(new PrimitiveType(Type.Repetition.OPTIONAL, primitiveTypeName, name, originalType));
        });
        return new MessageType(object.getClass().getSimpleName(), fields);
    }

    /**
     * 获取parquet的组
     *
     * @param messageType schema
     * @param object      对象
     * @return
     */
    public static Group getGroup(MessageType messageType, Object object) {
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(messageType);
        Group group = groupFactory.newGroup();

        Map<String, Object> cloumnValueMap = TableUtil.getCloumnValues(object, null);

        for (int i = 0; i < messageType.getFieldCount(); i++) {
            String fieldName = messageType.getFieldName(i);

            Object mapField = MapFieldUtil.getMapField(cloumnValueMap, fieldName);
            if (mapField == null || "".equalsIgnoreCase(mapField.toString())) continue;

            String primitiveTypeName = "", originalTypeName = "";
            Type type = messageType.getType(i);

            if (type.isPrimitive()) {
                primitiveTypeName = type.asPrimitiveType().getPrimitiveTypeName().name();
            }
            OriginalType originalType = type.getOriginalType();
            if (originalType != null) {
                originalTypeName = originalType.name();
            }
            switch (primitiveTypeName.toUpperCase()) {
                case "INT32":
                    group.append(fieldName, Integer.parseInt(mapField.toString()));
                    break;
                case "INT64":
                    if ("TIMESTAMP_MILLIS".equalsIgnoreCase(originalTypeName) || "TIME_MILLIS".equalsIgnoreCase(originalTypeName)) {
                        group.append(fieldName, ((Date) mapField).getTime());
                    } else {
                        group.append(fieldName, Long.parseLong(mapField.toString()));
                    }
                    break;
                case "INT96":
                    group.append(fieldName, getInt96Value((Date) mapField));
                    break;
                case "FLOAT":
                    group.append(fieldName, Float.parseFloat(mapField.toString()));
                    break;
                case "DOUBLE":
                    group.append(fieldName, Double.parseDouble(mapField.toString()));
                    break;
                case "BOOLEAN":
                    group.append(fieldName, Boolean.parseBoolean(mapField.toString()));
                    break;
                case "BINARY":
                    if (mapField instanceof Date) {
                        group.append(fieldName, DateUtils.formatDate((Date) mapField, "yyyy-MM-dd HH:mm:ss"));
                    } else {
                        group.append(fieldName, mapField.toString());
                    }
                    break;
                default:
                    group.append(fieldName, mapField.toString());
            }
        }
        return group;
    }

    /**
     * 写入parquet数据
     *
     * @param parquetFile datas 数据集合
     * @param parquetFile messageType parquet文件类型
     * @param parquetFile parquet 文件路径
     */
    public static void writeFile(List<Object> datas, MessageType messageType, String parquetFile, String compressionCodecName) {
        if (datas == null || datas.size() == 0) return;
        Configuration conf = new Configuration();

        Path path = new Path(parquetFile);
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        if (messageType == null) messageType = getMessageType(datas.get(0));
        GroupWriteSupport.setSchema(messageType, conf);

        ParquetWriter<Group> writer = null;
        try {
            CompressionCodecName compressionCodec = CompressionCodecName.UNCOMPRESSED;
            if ("SNAPPY".equalsIgnoreCase(compressionCodecName)) {
                compressionCodec = CompressionCodecName.SNAPPY;
            } else if ("GZIP".equalsIgnoreCase(compressionCodecName)) {
                compressionCodec = CompressionCodecName.GZIP;
            } else if ("LZO".equalsIgnoreCase(compressionCodecName)) {
                compressionCodec = CompressionCodecName.LZO;
            }
            writer = new ParquetWriter<Group>(path, writeSupport,
                    compressionCodec,
                    ParquetWriter.DEFAULT_BLOCK_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE,
                    ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                    ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                    ParquetProperties.WriterVersion.PARQUET_1_0, conf);
            ParquetWriter<Group> finalWriter = writer;
            for (Object object : datas) {
                try {
                    finalWriter.write(getGroup(messageType, object));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        } finally {
            try {
                if (writer != null) writer.close();
            } catch (IOException e) {
                log.error(e.getLocalizedMessage(), e);
            }
        }
    }

    /**
     * 将多个相同数据的parquet文件进行合并
     *
     * @param parquetFiles         要合并的文件集
     * @param mergeFile            合并之后文件
     * @param fileType             messageType parquet文件类型
     * @param compressionCodecName parquet 压缩方法
     * @param lineCount            最大文件行数
     */
    public static List<File> mergeFiles(List<File> parquetFiles, String mergeFile, String fileType, String compressionCodecName, int lineCount) {
        Configuration conf = new Configuration();
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetReader<Group> reader = null;
        ParquetWriter<Group> writer = null;
        CompressionCodecName compressionCodec = CompressionCodecName.UNCOMPRESSED;
        if ("SNAPPY".equalsIgnoreCase(compressionCodecName)) {
            compressionCodec = CompressionCodecName.SNAPPY;
        } else if ("GZIP".equalsIgnoreCase(compressionCodecName)) {
            compressionCodec = CompressionCodecName.GZIP;
        } else if ("LZO".equalsIgnoreCase(compressionCodecName)) {
            compressionCodec = CompressionCodecName.LZO;
        }
        AtomicLong counter = new AtomicLong(0);

        MessageType messageType = null;
        List<File> mergedFiles = new ArrayList<>();
        for (File parquetFile : parquetFiles) {
            try {
                if (StringUtils.isNotEmpty(fileType) && !parquetFile.getAbsolutePath().endsWith(fileType.toLowerCase())) {
                    continue;
                }

                //当收集的行数超过lineCount 则退出文件合并
                if (counter.get() >= lineCount) break;

                if (parquetFile.length() == 0) {
                    //读取的parquet文件是空文件
                    log.warn("delete empty parquet file:" + parquetFile.getAbsolutePath());
                    FileUtils.forceDelete(parquetFile);
                    continue;
                }
                try {
                    reader = ParquetReader.builder(readSupport, new Path(parquetFile.getAbsolutePath())).build();
                } catch (Exception ex) {
                    //该文件由于文件写入失败 而不是一个真正的parquet文件，目前直接删除
                    FileUtils.forceDelete(parquetFile);
                    log.warn("delete error parquet file:" + parquetFile.getAbsolutePath() + "," + ex.getLocalizedMessage());
                    continue;
                }

                Group group = null;
                while ((group = reader.read()) != null) {
                    if (messageType == null || writer == null) {
                        messageType = new MessageType(group.getType().getName(), group.getType().getFields());
                        GroupWriteSupport.setSchema(messageType, conf);
                        writer = new ParquetWriter<Group>(new Path(mergeFile), writeSupport,
                                compressionCodec,
                                ParquetWriter.DEFAULT_BLOCK_SIZE,
                                ParquetWriter.DEFAULT_PAGE_SIZE,
                                ParquetWriter.DEFAULT_PAGE_SIZE,
                                ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                                ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                                ParquetProperties.WriterVersion.PARQUET_1_0, conf);
                    }
                    //防止写入一条数据失败 导致所有的数据都失败
                    try {
                        writer.write(group);
                        counter.addAndGet(1L);
                    } catch (Exception ex) {
                        log.error(ex.getLocalizedMessage(), ex);
                    }
                }
                mergedFiles.add(parquetFile);
            } catch (Exception ex) {
                log.error(ex.getLocalizedMessage());
            } finally {
                IOUtils.closeQuietly(reader);
            }
        }
        IOUtils.closeQuietly(writer);
        return counter.get() > 0 ? mergedFiles : null;
    }

    /**
     * 读取parquet数据
     *
     * @param parquetFile parquet文件路径
     */
    public static List<Map<String, Object>> readFile(String parquetFile) {
        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetReader<Group> reader = null;
        List<Map<String, Object>> datas = new ArrayList<>();
        try {
            reader = ParquetReader.builder(readSupport, new Path(parquetFile)).build();
            List<Type> fields = reader.read().getType().getFields();
            log.info(JSON.toJSONString(fields));
            Group group = null;
            while ((group = reader.read()) != null) {
                Map<String, Object> dataMap = new HashMap<>();
                Group finalGroup = group;
                group.getType().getFields().forEach(type -> {
                    String primitiveTypeName = "", originalTypeName = "";
                    if (type.isPrimitive()) {
                        primitiveTypeName = type.asPrimitiveType().getPrimitiveTypeName().name();
                    }
                    OriginalType originalType = type.getOriginalType();
                    if (originalType != null) {
                        originalTypeName = originalType.name();
                    }
                    Object object = null;
                    try {
                        switch (primitiveTypeName.toUpperCase()) {
                            case "INT32":
                                object = finalGroup.getInteger(type.getName(), 0);
                                break;
                            case "INT64":
                                if ("TIMESTAMP_MILLIS".equalsIgnoreCase(originalTypeName) || "TIME_MILLIS".equalsIgnoreCase(originalTypeName)) {
                                    Calendar calendar = Calendar.getInstance();
                                    calendar.setTimeInMillis(finalGroup.getLong(type.getName(), 0));
                                    object = calendar.getTime();
                                } else {
                                    object = finalGroup.getLong(type.getName(), 0);
                                }
                                break;
                            case "INT96":
                                Calendar calendar = Calendar.getInstance();
                                calendar.setTimeInMillis(getTimestampMillis(finalGroup.getInt96(type.getName(), 0)));
                                object = calendar.getTime();
                                break;
                            case "FLOAT":
                                object = finalGroup.getFloat(type.getName(), 0);
                                break;
                            case "DOUBLE":
                                object = finalGroup.getDouble(type.getName(), 0);
                                break;
                            case "BOOLEAN":
                                object = finalGroup.getBoolean(type.getName(), 0);
                                break;
                            default:
                                object = finalGroup.getBinary(type.getName(), 0).toStringUsingUTF8();
                        }
                    } catch (Exception ex) {
                        //TODO 如果字段沒有值 会报错，暂时忽略这个错误
                    }
                    dataMap.put(type.getName(), object);
                });
                datas.add(dataMap);
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        } finally {
            try {
                if (reader != null) reader.close();
            } catch (IOException e) {
                log.error(e.getLocalizedMessage(), e);
            }
        }
        return datas;
    }

    public static long getTimestampMillis(Binary timestampBinary) {
        if (timestampBinary.length() != 12) {
            return 0;
        }
        byte[] bytes = timestampBinary.getBytes();

        // little endian encoding - need to invert byte order
        long timeOfDayNanos = Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
        int julianDay = Ints.fromBytes(bytes[11], bytes[10], bytes[9], bytes[8]);

        return (julianDay - 2440588) * TimeUnit.DAYS.toMillis(1) + (timeOfDayNanos / TimeUnit.MILLISECONDS.toNanos(1));
    }

    private static byte[] getBytes(int i) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) ((i >> 24) & 0xFF);
        bytes[1] = (byte) ((i >> 16) & 0xFF);
        bytes[2] = (byte) ((i >> 8) & 0xFF);
        bytes[3] = (byte) (i & 0xFF);
        return bytes;
    }

    private static byte[] getBytes(long i) {
        byte[] bytes = new byte[8];
        bytes[0] = (byte) ((i >> 56) & 0xFF);
        bytes[1] = (byte) ((i >> 48) & 0xFF);
        bytes[2] = (byte) ((i >> 40) & 0xFF);
        bytes[3] = (byte) ((i >> 32) & 0xFF);
        bytes[4] = (byte) ((i >> 24) & 0xFF);
        bytes[5] = (byte) ((i >> 16) & 0xFF);
        bytes[6] = (byte) ((i >> 8) & 0xFF);
        bytes[7] = (byte) (i & 0xFF);
        return bytes;
    }

    // 调转字节数组
    private static void flip(byte[] bytes) {
        for (int i = 0, j = bytes.length - 1; i < j; i++, j--) {
            byte t = bytes[i];
            bytes[i] = bytes[j];
            bytes[j] = t;
        }
    }

    // 每天的纳秒数
    private static final long NANO_SECONDS_PER_DAY = 86400_000_000_000L;
    // 儒略历起始日（儒略历的公元前4713年1月1日中午12点，在格里历是公元前4714年11月24日）距离1970-01-01的天数
    private static final long JULIAN_EPOCH_OFFSET_DAYS = 2440588;

    public static Binary getInt96Value(Date date) {
        // 写入数据，此处预先存在一个Date对象（可由时间戳转换得到）
        // 转换成距1970-01-01 00:00:00的纳秒数
        long nano = date.getTime() * 1000_000;
        // 转换成距儒略历起始日的天数
        int julianDays = (int) ((nano / NANO_SECONDS_PER_DAY) + JULIAN_EPOCH_OFFSET_DAYS);
        byte[] julianDaysBytes = getBytes(julianDays);
        flip(julianDaysBytes);
        // 当前时间戳距离当天已过去的纳秒数
        long lastDayNanos = nano % NANO_SECONDS_PER_DAY;
        byte[] lastDayNanosBytes = getBytes(lastDayNanos);
        flip(lastDayNanosBytes);
        byte[] dst = new byte[12];
        // 前8字节表示时间戳对应当天已过去的纳秒数
        System.arraycopy(lastDayNanosBytes, 0, dst, 0, 8);
        // 后4字节表示时间戳当天距离儒略历起始日已过去的天数
        System.arraycopy(julianDaysBytes, 0, dst, 8, 4);
        // Group group = factory.newGroup();
        return Binary.fromByteArray(dst);
    }
}
