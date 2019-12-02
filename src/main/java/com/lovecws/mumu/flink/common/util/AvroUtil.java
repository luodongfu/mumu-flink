package com.lovecws.mumu.flink.common.util;

import com.lovecws.mumu.flink.common.annotation.AvroField;
import com.alibaba.fastjson.JSON;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Deflater;

/**
 * @program: act-able
 * @description: avro工具类
 * @author: 甘亮
 * @create: 2019-06-06 10:04
 **/
public class AvroUtil {

    private static final Logger log = Logger.getLogger(AvroUtil.class);

    private static Object[] getAvroType(String typeName) {
        Object[] avroType = null;
        if ("Integer".equalsIgnoreCase(typeName) || "int".equalsIgnoreCase(typeName)) {
            avroType = new Object[]{"int", "null"};
        } else if ("Long".equalsIgnoreCase(typeName) || "long".equalsIgnoreCase(typeName)) {
            avroType = new Object[]{"long", "null", "int"};
        } else if ("Float".equalsIgnoreCase(typeName) || "float".equalsIgnoreCase(typeName)) {
            avroType = new Object[]{"double", "null"};
        } else if ("Boolean".equalsIgnoreCase(typeName) || "boolean".equalsIgnoreCase(typeName)) {
            avroType = new Object[]{"boolean", "null"};
        } else if ("Date".equalsIgnoreCase(typeName) || "timestamp".equalsIgnoreCase(typeName)) {
            Map<String, String> typeMap = new HashMap<>();
            typeMap.put("type", "long");
            typeMap.put("ogicalType", "timestamp-millis");
            avroType = new Object[]{typeMap, "null"};
        } else {
            avroType = new Object[]{"string", "null"};
        }
        return avroType;
    }

    /**
     * 从模型类中获取avro的schema
     *
     * @param data 事件类型
     * @return
     */
    public static Schema getSchema(Object data) {
        if (data == null) throw new IllegalArgumentException();
        Class clazz = null;
        if (data instanceof Class) {
            clazz = (Class) data;
        } else {
            clazz = data.getClass();
        }

        List<Map<String, Object>> fields = new ArrayList<>();
        if (data instanceof Map) {
            List<Map<String, Object>> keys = MapFieldUtil.getFlatMapKeys((Map<String, Object>) data);
            keys.forEach(map -> {
                Map<String, Object> fieldMap = new HashMap<>();
                fieldMap.put("name", map.get("name").toString());
                fieldMap.put("type", getAvroType(map.get("type").toString()));
                fields.add(fieldMap);
            });
        } else if (data instanceof List) {
            List<Map<String, String>> columnFields = (List<Map<String, String>>) data;
            columnFields.forEach(flatMap -> {
                String name = flatMap.get("name");
                String type = flatMap.get("type").toLowerCase();
                Map<String, Object> fieldMap = new HashMap<>();
                fieldMap.put("name", name);
                fieldMap.put("type", getAvroType(type));
                fieldMap.put("default", null);
                fieldMap.put("doc", "");
                fields.add(fieldMap);
            });
        } else {
            Field[] avroFields = FieldUtils.getFieldsWithAnnotation(clazz, AvroField.class);
            if (avroFields == null || avroFields.length == 0) {
                Field[] commonFields = FieldUtils.getAllFields(clazz);
                for (Field field : commonFields) {
                    field.setAccessible(true);
                    Map<String, Object> fieldMap = new HashMap<>();
                    fieldMap.put("name", field.getName());
                    fieldMap.put("type", getAvroType(field.getType().getSimpleName()));
                    fieldMap.put("default", null);
                    fieldMap.put("doc", "");
                    fields.add(fieldMap);
                }
            } else {
                for (Field avroField : avroFields) {
                    avroField.setAccessible(true);
                    AvroField annotation = avroField.getAnnotation(AvroField.class);
                    Map<String, Object> fieldMap = new HashMap<>();
                    fieldMap.put("name", annotation.name());
                    fieldMap.put("type", annotation.type());
                    fieldMap.put("default", null);
                    fieldMap.put("doc", "");
                    fields.add(fieldMap);
                }
            }
        }

        Map<String, Object> schemaMap = new HashMap<>();
        schemaMap.put("type", "record");
        schemaMap.put("name", clazz.getSimpleName());
        schemaMap.put("namespace", clazz.getName());
        schemaMap.put("fields", fields);

        return Schema.parse(JSON.toJSONString(schemaMap));
    }

    /**
     * 获取到avro的record
     *
     * @param data   对象
     * @param schema schema信息
     * @return
     */
    public static GenericRecord getRecord(Object data, Schema schema) {
        if (data == null) throw new IllegalArgumentException();
        if (schema == null) schema = getSchema(data);

        GenericRecord record = new GenericData.Record(schema);
        List<Schema.Field> fields = record.getSchema().getFields();

        Field[] avroFields = FieldUtils.getFieldsWithAnnotation(data.getClass(), AvroField.class);
        Field[] allFields = FieldUtils.getAllFields(data.getClass());

        try {
            for (Schema.Field field : fields) {
                String name = field.name();
                if (data instanceof Map) {
                    Object mapField = MapFieldUtil.getMapField((Map) data, name, "");
                    if (mapField != null && StringUtils.isNotEmpty(mapField.toString())) {
                        if (mapField instanceof Date) {
                            mapField = ((Date) mapField).getTime();
                        }
                        record.put(name, mapField);
                    }
                } else {
                    if (avroFields != null && avroFields.length > 0) {
                        for (Field avroField : avroFields) {
                            avroField.setAccessible(true);
                            if (avroField.getAnnotation(AvroField.class).name().equalsIgnoreCase(name)) {
                                Object o = avroField.get(data);
                                if (o != null) {
                                    if (o instanceof Date) {
                                        o = ((Date) o).getTime();
                                    }
                                    record.put(name, o);
                                }
                            }
                        }
                    } else {
                        for (Field commonField : allFields) {
                            commonField.setAccessible(true);
                            if (commonField.getName().equalsIgnoreCase(name)) {
                                Object o = commonField.get(data);
                                if (o != null) {
                                    if (o instanceof Date) {
                                        o = ((Date) o).getTime();
                                    }
                                    record.put(name, o);
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        }
        return record;
    }

    /**
     * 读取avro数据
     *
     * @param avroFile avro文件
     * @return 集合
     */
    public static List<Map<String, Object>> readFile(String avroFile) {
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
        DataFileReader<GenericRecord> dataFileReader = null;
        List<Map<String, Object>> datas = new ArrayList<>();
        try {
            dataFileReader = new DataFileReader<GenericRecord>(new File(avroFile), reader);
            while (dataFileReader.hasNext()) {
                GenericRecord genericRecord = dataFileReader.next();
                Map<String, Object> recordMap = new HashMap<>();
                for (Schema.Field field : genericRecord.getSchema().getFields()) {
                    Object o = genericRecord.get(field.name());
                    if (o != null && StringUtils.isNotEmpty(o.toString())) {
                        //处理日期格式数据
                        for (Schema schema : field.schema().getTypes()) {
                            String ogicalType = schema.getProp("ogicalType");
                            if ("timestamp-millis".equalsIgnoreCase(ogicalType)) {
                                Calendar calendar = Calendar.getInstance();
                                calendar.setTimeInMillis(Long.parseLong(o.toString()));
                                o = calendar.getTime();
                            }
                        }
                        if (o instanceof Utf8) {
                            o = ((Utf8) o).toString();
                        }
                        recordMap.put(field.name(), o);
                    }
                }
                datas.add(recordMap);
            }
        } catch (IOException ex) {
            log.error(ex.getLocalizedMessage(), ex);
        } finally {
            IOUtils.closeQuietly(dataFileReader);
        }
        return datas;
    }

    /**
     * 将数据写入到avro文件中
     *
     * @param values           值
     * @param columnFields     字段信息
     * @param filePath         文件路径
     * @param compressionCodec 压缩方式 null deflate snappy bzip2
     */
    public static void writeFile(List<Map<String, Object>> values, List<Map<String, String>> columnFields, String filePath, String compressionCodec) {
        if (values == null) return;

        DataFileWriter<Object> dataFileWriter = null;
        DataFileWriter<Object> avroWriter = null;
        try {
            Schema schema = AvroUtil.getSchema(columnFields);
            dataFileWriter = new DataFileWriter<Object>(new GenericDatumWriter<>(schema));
            CodecFactory codecFactory = CodecFactory.nullCodec();
            if ("snappy".equalsIgnoreCase(compressionCodec)) {
                codecFactory = CodecFactory.snappyCodec();
            } else if ("deflate".equalsIgnoreCase(compressionCodec)) {
                codecFactory = CodecFactory.deflateCodec(Deflater.DEFAULT_COMPRESSION);
            } else if ("bzip2".equalsIgnoreCase(compressionCodec)) {
                codecFactory = CodecFactory.bzip2Codec();
            }
            dataFileWriter.setCodec(codecFactory);

            avroWriter = dataFileWriter.create(schema, new File(filePath));
            for (Map<String, Object> valueMap : values) {
                avroWriter.append(AvroUtil.getRecord(valueMap, schema));
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        } finally {
            if (avroWriter != null) IOUtils.closeQuietly(avroWriter);
            if (dataFileWriter != null) IOUtils.closeQuietly(dataFileWriter);
        }
    }

    /**
     * 将多个avro文件合并成一个avro文件
     *
     * @param avroFiles        文件集合
     * @param mergeFile        合并的文件
     * @param fileType         文件类型
     * @param compressionCodec 压缩方式
     * @param mergeLineCount   最大合并的行数
     */
    public static List<File> mergeFiles(List<File> avroFiles, String mergeFile, String fileType, String compressionCodec, int mergeLineCount) {
        DataFileWriter<Object> dataFileWriter = null;
        DataFileWriter<Object> avroWriter = null;

        DataFileReader<GenericRecord> dataFileReader = null;
        Schema schema = null;
        AtomicLong counter = new AtomicLong();

        List<File> mergedFiles = new ArrayList<>();
        for (File avroFile : avroFiles) {
            try {
                //过滤非该文件类型的文件
                if (StringUtils.isNotEmpty(fileType) && !avroFile.getAbsolutePath().endsWith(fileType.toLowerCase())) {
                    continue;
                }
                if (counter.get() >= mergeLineCount) break;

                DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
                dataFileReader = new DataFileReader<GenericRecord>(avroFile, reader);
                if (schema == null) {
                    schema = dataFileReader.getSchema();
                    dataFileWriter = new DataFileWriter<Object>(new GenericDatumWriter<>(schema));
                    CodecFactory codecFactory = CodecFactory.nullCodec();
                    if ("snappy".equalsIgnoreCase(compressionCodec)) {
                        codecFactory = CodecFactory.snappyCodec();
                    } else if ("deflate".equalsIgnoreCase(compressionCodec)) {
                        codecFactory = CodecFactory.deflateCodec(Deflater.DEFAULT_COMPRESSION);
                    } else if ("bzip2".equalsIgnoreCase(compressionCodec)) {
                        codecFactory = CodecFactory.bzip2Codec();
                    }
                    dataFileWriter.setCodec(codecFactory);
                    avroWriter = dataFileWriter.create(schema, new File(mergeFile));
                }
                while (dataFileReader.hasNext()) {
                    try {
                        avroWriter.append(dataFileReader.next());
                        counter.addAndGet(1L);
                    } catch (Exception ex) {
                        log.error(ex.getLocalizedMessage(), ex);
                    }
                }
                mergedFiles.add(avroFile);
            } catch (Exception ex) {
                log.error(ex.getLocalizedMessage(), ex);
            } finally {
                IOUtils.closeQuietly(dataFileReader);
            }
        }
        IOUtils.closeQuietly(avroWriter);
        IOUtils.closeQuietly(dataFileWriter);
        return counter.get() > 0 ? mergedFiles : null;
    }

    /**
     * 获取单条avro的schema字节数组
     *
     * @param object 对象信息
     * @param schema schema
     * @return 字节数组
     */
    public static byte[] getBytes(Object object, Schema schema) {
        if (schema == null) schema = getSchema(object);
        GenericRecord record = AvroUtil.getRecord(object, schema);
        return GenericAvroCodecs.apply(schema).apply(record);
    }
}
