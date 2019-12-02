package com.lovecws.mumu.flink.streaming.common.factory;

import com.lovecws.mumu.flink.common.util.AvroUtil;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: trunk
 * @description: avro工厂
 * @author: 甘亮
 * @create: 2019-11-05 16:42
 **/
public class AvroFactory<IN> implements BulkWriter.Factory<IN> {

    private transient Schema schema;

    public AvroFactory(Schema schema) {
        this.schema = schema;
    }

    @Override
    public BulkWriter<IN> create(FSDataOutputStream out) throws IOException {
        DataFileWriter<Object> dataFileWriter = new DataFileWriter<Object>(new GenericDatumWriter<>(schema));
        DataFileWriter<Object> avroWriter = dataFileWriter.create(schema, out);
        return new ParquetWriter<IN>(avroWriter, schema);
    }

    public static class ParquetWriter<Object> implements BulkWriter<Object> {

        private DataFileWriter<java.lang.Object> avroWriter;
        private Schema schema;

        private ParquetWriter(DataFileWriter<java.lang.Object> avroWriter, Schema schema) {
            this.avroWriter = avroWriter;
            this.schema = schema;
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
                avroWriter.append(AvroUtil.getRecord(data, schema));
            }
        }

        @Override
        public void flush() throws IOException {
            avroWriter.flush();
        }

        @Override
        public void finish() throws IOException {
            avroWriter.flush();
            avroWriter.sync();
        }
    }
}
