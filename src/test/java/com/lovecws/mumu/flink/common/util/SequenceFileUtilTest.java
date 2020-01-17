package com.lovecws.mumu.flink.common.util;

import com.alibaba.fastjson.JSON;
import com.lovecws.mumu.flink.common.model.gynetres.GynetresModel;
import org.junit.Test;

import java.util.*;

/**
 * @program: mumu-flink
 * @description: SequenceFileUtil工具类测试
 * @author: 甘亮
 * @create: 2019-06-21 15:25
 **/
public class SequenceFileUtilTest {

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

        SequenceFileUtil.writeFile(Arrays.asList(cloumnValues, cloumnValues, cloumnValues, cloumnValues), tableFields, "E:\\data\\dataprocessing\\util\\1.sequence", null);
    }

    @Test
    public void readFile() {
        String sequecneFilePath = "E:\\data\\dataprocessing\\util\\1.sequence";
        List<Map<String, Object>> datas = SequenceFileUtil.readFile(sequecneFilePath);
        System.out.println(JSON.toJSONString(datas));
    }
}
