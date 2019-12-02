package com.lovecws.mumu.flink.streaming.common.encoder;

import com.lovecws.mumu.flink.common.util.TxtUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: act-able
 * @description: csv输出格式
 * @author: 甘亮
 * @create: 2019-11-13 16:27
 **/
@Slf4j
public class CsvEncoder extends SimpleStringEncoder<Object> {

    private String sinkName;//sink存储的类名称
    private String path;//写入的文件路径
    private String[] fields;//写入文件的全字段
    private String[] uploadFields;//需要写入值的字段列表

    public CsvEncoder(String sinkName, String path, String[] fields, String[] uploadFields) {
        this.sinkName = sinkName;
        this.path = path;
        this.fields = fields;
        this.uploadFields = uploadFields;
    }

    @Override
    public void encode(Object element, OutputStream stream) throws IOException {
        List<Object> datas = new ArrayList<>();
        if (element instanceof List) {
            datas.addAll((List) element);
        } else {
            datas.add(element);
        }
        //输出csv格式数据
        for (Object data : datas) {
            super.encode(TxtUtils.readLine(data, fields, uploadFields), stream);
        }
        log.info(sinkName + ":{path:" + path + ",fileType:" + ",size:" + datas.size() + "}");
    }
}
