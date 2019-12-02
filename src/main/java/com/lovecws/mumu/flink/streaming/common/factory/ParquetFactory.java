package com.lovecws.mumu.flink.streaming.common.factory;

import com.lovecws.mumu.flink.common.util.ParquetUtil;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: trunk
 * @description: parquet格式化
 * @author: 甘亮
 * @create: 2019-11-05 16:42
 **/
public class ParquetFactory<IN> implements BulkWriter.Factory<IN> {

    private transient MessageType messageType;

    public ParquetFactory(MessageType messageType) {
        this.messageType = messageType;
    }

    @Override
    public BulkWriter<IN> create(FSDataOutputStream out) throws IOException {
        Configuration conf = new Configuration();
        GroupWriteSupport.setSchema(messageType, conf);

        ParquetWriter<Group> parquetWriter = new ParquetWriter<Group>(new Path(""), new GroupWriteSupport(),
                ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
                ParquetWriter.DEFAULT_BLOCK_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                ParquetWriter.DEFAULT_PAGE_SIZE,
                ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                ParquetProperties.WriterVersion.PARQUET_1_0, conf);
        return new ParquetWriter2<IN>(parquetWriter, messageType);
    }

    public static class ParquetWriter2<Object> implements BulkWriter<Object> {

        private ParquetWriter<Group> parquetWriter;
        private MessageType messageType;

        private ParquetWriter2(ParquetWriter<Group> parquetWriter, MessageType messageType) {
            this.parquetWriter = parquetWriter;
            this.messageType = messageType;
        }

        @Override
        public void addElement(Object element) throws IOException {
            List<Object> datas = new ArrayList<>();
            if (element instanceof List) {
                datas.addAll((List) element);
            } else {
                datas.add(element);
            }
            for (Object data : datas) {
                parquetWriter.write(ParquetUtil.getGroup(messageType, data));
            }
        }

        @Override
        public void flush() throws IOException {
        }

        @Override
        public void finish() throws IOException {
        }
    }
}
