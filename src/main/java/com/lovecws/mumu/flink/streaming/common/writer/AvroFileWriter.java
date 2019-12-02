package com.lovecws.mumu.flink.streaming.common.writer;

import com.lovecws.mumu.flink.common.util.AvroUtil;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.streaming.connectors.fs.StreamWriterBase;
import org.apache.flink.streaming.connectors.fs.Writer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: act-able
 * @description: avro写入器
 * @author: 甘亮
 * @create: 2019-11-27 17:02
 **/
public class AvroFileWriter<T> extends StreamWriterBase<T> {

    private transient DataFileWriter<Object> avroWriter = null;
    private transient Schema schema;

    //avro编码 snappy、null、deflate、bzip2
    private String codec;

    private AvroFileWriter(AvroFileWriter other) {
        setSyncOnFlush(other.isSyncOnFlush());
        this.codec = other.codec;
    }

    public AvroFileWriter(String codec) {
        this(true, codec);
    }

    public AvroFileWriter(boolean syncOnFlush, String codec) {
        setSyncOnFlush(syncOnFlush);
        this.codec = codec;
    }

    @Override
    public void open(FileSystem fs, Path path) throws IOException {
        super.open(fs, path);
    }

    @Override
    public void close() throws IOException {
        super.close();
        avroWriter.close();
        avroWriter = null;
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
            if (avroWriter == null) {
                schema = AvroUtil.getSchema(data);
                DataFileWriter<Object> dataFileWriter = new DataFileWriter<Object>(new GenericDatumWriter<>(schema));
                dataFileWriter.setCodec(getAvroCodecFactory(codec));
                avroWriter = dataFileWriter.create(schema, getStream());
            }
            GenericRecord record = AvroUtil.getRecord(data, schema);
            avroWriter.append(record);
        }
    }

    @Override
    public Writer<T> duplicate() {
        return new AvroFileWriter<T>(this);
    }

    @Override
    public long flush() throws IOException {
        avroWriter.flush();
        return super.flush();
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
            codecFactory = CodecFactory.nullCodec();
        }
        return codecFactory;
    }
}
