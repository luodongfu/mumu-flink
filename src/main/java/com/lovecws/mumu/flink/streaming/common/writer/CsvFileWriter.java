package com.lovecws.mumu.flink.streaming.common.writer;

import com.lovecws.mumu.flink.common.util.TxtUtils;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: act-able
 * @description: csv文件写入
 * @author: 甘亮
 * @create: 2019-11-27 18:41
 **/
public class CsvFileWriter<T> extends StringWriter<T> {

    private String[] fileds;
    private String[] uploadFields;

    public CsvFileWriter(String[] fileds, String[] uploadFields) {
        this.fileds = fileds;
        this.uploadFields = uploadFields;
    }

    public CsvFileWriter(CsvFileWriter<T> other) {
        this.fileds = other.fileds;
        this.uploadFields = other.uploadFields;
    }

    @Override
    public void write(T value) throws IOException {
        List<T> datas = new ArrayList<>();
        if (value instanceof List) {
            datas.addAll((List) value);
        } else {
            datas.add(value);
        }
        FSDataOutputStream outputStream = getStream();
        datas.forEach(data -> {
            try {
                outputStream.write(TxtUtils.readLine(data, fileds, uploadFields).getBytes());
                outputStream.write('\n');
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public CsvFileWriter<T> duplicate() {
        return new CsvFileWriter<T>(this);
    }
}
