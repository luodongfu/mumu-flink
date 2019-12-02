package com.lovecws.mumu.flink.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.lovecws.mumu.flink.common.model.atd.AtdEventModel;
import com.lovecws.mumu.flink.common.model.gynetres.GynetresModel;
import org.apache.avro.Schema;
import org.junit.Test;

import java.util.*;

/**
 * @program: act-able
 * @description: AvroUtil测试类
 * @author: 甘亮
 * @create: 2019-06-21 09:28
 **/
public class AvroUtilTest {

    @Test
    public void writeFile() {

        GynetresModel gynetresModel = new GynetresModel();
        gynetresModel.setDs(DateUtils.formatDate(new Date(), "yyyy-MM-dd"));
        gynetresModel.setId(UUID.randomUUID().toString().replace("-", ""));
        gynetresModel.setProcessTime(new Date());
        gynetresModel.setCreateTime(new Date());
        gynetresModel.setCounter(1L);
        gynetresModel.setPort("80");

        List<Map<String, String>> tableFields = TableUtil.getTableFields(gynetresModel);
        Map<String, Object> cloumnValues = TableUtil.getCloumnValues(gynetresModel, tableFields);

        AvroUtil.writeFile(Arrays.asList(cloumnValues, cloumnValues, cloumnValues, cloumnValues), tableFields, "E:\\data\\dataprocessing\\util\\1.avro", null);
    }

    @Test
    public void readFile() {
        String avroPath = "E:\\data\\dataprocessing\\util\\000000_0";
        avroPath = "E:\\data\\dataprocessing\\util\\1.avro";
        avroPath = "E:\\data\\mumuflink\\atd\\basefile\\20191120\\201911202052308001c2ba1.avro";
        avroPath = "E:\\data\\mumuflink\\atd\\localfile\\2019112719\\part-0-0.avro";
        List<Map<String, Object>> maps = AvroUtil.readFile(avroPath);
        maps.forEach(map -> System.out.println(JSON.toJSONString(map, SerializerFeature.WriteDateUseDateFormat)));
        System.out.println(maps.size());
    }

    @Test
    public void getSchema() {
        Schema schema = AvroUtil.getSchema(new AtdEventModel());
        System.out.println(schema);
    }
}
