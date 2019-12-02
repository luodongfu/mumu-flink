package com.lovecws.mumu.flink.streaming.common.writer;

import com.lovecws.mumu.flink.common.util.ParquetUtil;
import org.apache.flink.streaming.connectors.fs.StreamWriterBase;
import org.apache.flink.streaming.connectors.fs.Writer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: act-able
 * @description: parquet的写入器
 * @author: 甘亮
 * @create: 2019-11-27 17:02
 **/
public class ParquetFileWriter<T> extends StreamWriterBase<T> {

    private transient ParquetWriter<Group> parquetWriter = null;
    private transient MessageType messageType = null;
    private transient Path path = null;

    //avro编码 snappy、gzip、uncompressed、lzo
    private String codec;

    private ParquetFileWriter(ParquetFileWriter other) {
        this.codec = other.codec;
    }

    public ParquetFileWriter(String codec) {
        this.codec = codec;
    }

    @Override
    public long getPos() throws IOException {
        return parquetWriter.getDataSize();
    }

    @Override
    public void open(FileSystem fs, Path path) throws IOException {
        this.path = path;
    }

    @Override
    public void close() throws IOException {
        parquetWriter.close();
        parquetWriter = null;
    }

    @Override
    public void write(Object value) throws IOException {
        List<Object> datas = new ArrayList<>();
        if (value instanceof List) {
            datas.addAll((List) value);
        } else {
            datas.add(value);
        }
        for (Object data : datas) {
            if (parquetWriter == null) {
                messageType = ParquetUtil.getMessageType(data);
                Configuration conf = new Configuration();
                GroupWriteSupport.setSchema(messageType, conf);
                parquetWriter = new ParquetWriter<Group>(path, new GroupWriteSupport(),
                        getParquetCompressionCodecName(codec),
                        ParquetWriter.DEFAULT_BLOCK_SIZE,
                        ParquetWriter.DEFAULT_PAGE_SIZE,
                        ParquetWriter.DEFAULT_PAGE_SIZE,
                        ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                        ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                        ParquetProperties.WriterVersion.PARQUET_1_0, conf);
            }
            Group group = ParquetUtil.getGroup(messageType, data);
            parquetWriter.write(group);
        }
    }

    @Override
    public Writer<T> duplicate() {
        return new ParquetFileWriter<T>(this);
    }

    @Override
    public long flush() throws IOException {
        return parquetWriter.getDataSize();
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
            compressionCodecName = CompressionCodecName.UNCOMPRESSED;
        }
        return compressionCodecName;
    }
}
