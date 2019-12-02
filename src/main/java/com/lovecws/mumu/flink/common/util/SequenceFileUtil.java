package com.lovecws.mumu.flink.common.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.*;

/**
 * @program: act-able
 * @description: SequenceFile工具类，对
 * @author: 甘亮
 * @create: 2019-06-21 14:36
 **/
public class SequenceFileUtil {

    private static final Logger log = Logger.getLogger(SequenceFileUtil.class);

    //hive默认文件分隔符
    private static final String SEQUENCEFILE_SEPEATOR = "\u0001";

    /**
     * 读取sequencefile文件信息
     *
     * @param sequenceFile 文件路径
     * @return
     */
    public static List<Map<String, Object>> readFile(String sequenceFile) {
        Configuration conf = new Configuration();

        SequenceFile.Reader reader = null;
        List<Map<String, Object>> datas = new ArrayList<>();
        try {
            reader = new SequenceFile.Reader(FileSystem.get(conf), new Path(sequenceFile), conf);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

            //metadata 包含字段信息
            TreeMap<Text, Text> metadata = reader.getMetadata().getMetadata();
            while (reader.next(key, value)) {
                Map<String, Object> map = new HashMap<>();
                String[] fields = value.toString().split(SEQUENCEFILE_SEPEATOR);

                if (metadata.size() == 0) {
                    for (int i = 0; i < fields.length; i++) {
                        map.put(String.valueOf(i), fields[i]);
                    }
                } else {
                    int i = 0;
                    for (Iterator<Map.Entry<Text, Text>> iterator = metadata.entrySet().iterator(); iterator.hasNext(); ) {
                        if (i < fields.length) {
                            map.put(iterator.next().getKey().toString(), fields[i]);
                            i++;
                        } else {
                            break;
                        }
                    }
                }
                datas.add(map);
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        } finally {
            IOUtils.closeStream(reader);
        }
        return datas;
    }

    /**
     * 写入sequenceFile文件信息
     *
     * @param values           值
     * @param columnFields     字段列表
     * @param filePath         写入文件路径
     * @param compressionCodec 压缩格式
     */
    public static void writeFile(List<Map<String, Object>> values, List<Map<String, String>> columnFields, String filePath, String compressionCodec) {
        Configuration conf = new Configuration();

        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = null;
        try {
            CompressionCodec codec = null;
            if ("snappy".equalsIgnoreCase(compressionCodec)) {
                codec = new SnappyCodec();
            } else if ("gzip".equalsIgnoreCase(compressionCodec)) {
                codec = new GzipCodec();
            } else if ("default".equalsIgnoreCase(compressionCodec)) {
                codec = new DefaultCodec();
            } else if ("bzip2".equalsIgnoreCase(compressionCodec)) {
                codec = new BZip2Codec();
            }

            //metadata元数据信息
            TreeMap<Text, Text> treeMap = new TreeMap<>();
            columnFields.forEach(columnFieldMap -> treeMap.put(new Text(columnFieldMap.get("name")), new Text(columnFieldMap.get("type"))));

            writer = SequenceFile.createWriter(FileContext.getFileContext(),
                    conf, new Path(filePath),
                    key.getClass(), value.getClass(), SequenceFile.CompressionType.BLOCK,
                    codec, new SequenceFile.Metadata(treeMap), EnumSet.of(CreateFlag.CREATE, CreateFlag.APPEND));

            for (int i = 0; i < values.size(); i++) {
                Map<String, Object> valueMap = values.get(i);
                List<Object> fieldValues = new ArrayList<>();

                for (Iterator<Map.Entry<Text, Text>> iterator = treeMap.entrySet().iterator(); iterator.hasNext(); ) {
                    Object o = valueMap.get(iterator.next().getKey().toString());
                    if (o == null) o = "";
                    fieldValues.add(o);
                }

                key.set(i);
                value.set(StringUtils.join(fieldValues, SEQUENCEFILE_SEPEATOR));
                writer.append(key, value);
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        } finally {
            IOUtils.closeStream(writer);
        }
    }

    /**
     * 将多个相同类型的sequncefile进行合并成一个sequencefile文件
     *
     * @param sequenceFiles    sequencefile文件列表
     * @param mergeFile        合并文件名称
     * @param fileType         文件类型
     * @param compressionCodec 压缩方式
     */
    public static void mergeFiles(List<File> sequenceFiles, String mergeFile, String fileType, String compressionCodec) {
        Configuration conf = new Configuration();
        SequenceFile.Writer writer = null;
        try {
            CompressionCodec codec = null;
            if ("snappy".equalsIgnoreCase(compressionCodec)) {
                codec = new SnappyCodec();
            } else if ("gzip".equalsIgnoreCase(compressionCodec)) {
                codec = new GzipCodec();
            } else if ("default".equalsIgnoreCase(compressionCodec)) {
                codec = new DefaultCodec();
            } else if ("bzip2".equalsIgnoreCase(compressionCodec)) {
                codec = new BZip2Codec();
            }

            for (File sequenceFile : sequenceFiles) {
                if (StringUtils.isNotEmpty(fileType) && !sequenceFile.getAbsolutePath().endsWith(fileType.toLowerCase())) {
                    continue;
                }
                SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), new Path(sequenceFile.getAbsolutePath()), conf);
                Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
                if (writer == null) {
                    writer = SequenceFile.createWriter(FileContext.getFileContext(),
                            conf, new Path(mergeFile),
                            key.getClass(), value.getClass(), SequenceFile.CompressionType.RECORD,
                            codec, new SequenceFile.Metadata(), EnumSet.of(CreateFlag.OVERWRITE));
                }
                while (reader.next(key, value)) {
                    writer.append(key, value);
                }
                IOUtils.closeStream(reader);
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        } finally {
            IOUtils.closeStream(writer);
        }
    }
}
