package com.lovecws.mumu.flink.streaming.common.writer;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.connectors.fs.StreamWriterBase;
import org.apache.flink.streaming.connectors.fs.Writer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.postgresql.shaded.com.ongres.scram.common.util.StringWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: act-able
 * @description: sequencefile写入器
 * @author: 甘亮
 * @create: 2019-11-27 17:32
 **/
public class SequencefileWriter<T> extends StreamWriterBase<T> {

    //压缩编码 None,snappy,lz4,bzip2
    private final String compressionCodecName;

    // 压缩类型 NONE RECORD BLOCK
    private SequenceFile.CompressionType compressionType;

    private transient SequenceFile.Writer writer;

    public SequencefileWriter() {
        this("None", SequenceFile.CompressionType.NONE);
    }

    public SequencefileWriter(String compressionCodecName,
                              SequenceFile.CompressionType compressionType) {
        if (StringUtils.isEmpty(compressionCodecName)) compressionCodecName = "None";
        this.compressionCodecName = compressionCodecName;

        this.compressionType = compressionType;
    }

    protected SequencefileWriter(SequencefileWriter<T> other) {
        this.compressionCodecName = other.compressionCodecName;
        this.compressionType = other.compressionType;
    }

    @Override
    public void open(FileSystem fs, Path path) throws IOException {
        super.open(fs, path);

        CompressionCodec codec = null;
        Configuration conf = fs.getConf();
        if (!compressionCodecName.equals("None")) {
            CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
            codec = codecFactory.getCodecByName(compressionCodecName);
            if (codec == null) {
                throw new RuntimeException("Codec " + compressionCodecName + " not found.");
            }
        }

        writer = SequenceFile.createWriter(conf,
                getStream(),
                LongWritable.class,
                StringWritable.class,
                compressionType,
                codec);

    }

    @Override
    public void close() throws IOException {
        if (writer != null) {
            writer.close();
        }
        super.close();
    }

    @Override
    public void write(T value) throws IOException {
        getStream();
        List<Object> datas = new ArrayList<>();
        if (value instanceof List) {
            datas.addAll((List) value);
        } else {
            datas.add(value);
        }
        for (Object data : datas) {
            writer.append(new LongWritable(Math.abs(data.hashCode())), new Text(data.toString()));
        }
    }

    @Override
    public Writer<T> duplicate() {
        return new SequencefileWriter<T>(this);
    }
}
