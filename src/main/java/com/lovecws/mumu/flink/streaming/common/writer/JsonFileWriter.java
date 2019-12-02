package com.lovecws.mumu.flink.streaming.common.writer;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: act-able
 * @description: json文件写入
 * @author: 甘亮
 * @create: 2019-11-27 18:01
 **/
public class JsonFileWriter<T> extends StringWriter<T> {

    public JsonFileWriter() {
    }

    public JsonFileWriter(JsonFileWriter other) {

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
                String jsonString = JSON.toJSONString(data);
                outputStream.write(jsonString.getBytes(StandardCharsets.UTF_8));
                outputStream.write('\n');
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public JsonFileWriter<T> duplicate() {
        return new JsonFileWriter<T>(this);
    }
}
