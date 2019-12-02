package com.lovecws.mumu.flink.streaming.common.encoder;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: trunk
 * @description: 文件保存的时候批量保存
 * @author: 甘亮
 * @create: 2019-11-05 09:55
 **/
@Slf4j
public class BatchStringEncoder extends SimpleStringEncoder<Object> {

    @Override
    public void encode(Object element, OutputStream stream) throws IOException {
        List<Object> elements = new ArrayList<>();
        if (element instanceof List) {
            elements.addAll((List) element);
        } else {
            elements.add(element);
        }
        for (Object ele : elements) {
            Object _data = "";
            if (ele instanceof String) {
                _data = ele;
            } else {
                try {
                    _data = JSON.toJSONString(ele);
                } catch (Exception ex) {
                    log.error(ex.getLocalizedMessage());
                }
            }
            super.encode(_data, stream);
        }
    }
}
