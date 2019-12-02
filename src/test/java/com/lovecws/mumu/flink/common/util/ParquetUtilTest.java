package com.lovecws.mumu.flink.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.lovecws.mumu.flink.common.model.attack.AttackEventModel;
import com.lovecws.mumu.flink.common.model.gynetres.GynetresModel;
import org.apache.parquet.schema.MessageType;
import org.junit.Test;

import java.io.File;
import java.util.*;

/**
 * @program: act-able
 * @description: parquet列式文件存储
 * @author: 甘亮
 * @create: 2019-06-12 09:07
 **/
public class ParquetUtilTest {

    @Test
    public void getMessageType() {
        MessageType messageType = ParquetUtil.getMessageType(new AttackEventModel());
        System.out.println(messageType);
    }

    @Test
    public void writeFile() {
        MessageType messageType = ParquetUtil.getMessageType(new GynetresModel());

        GynetresModel gynetresModel = new GynetresModel();
        gynetresModel.setDs(DateUtils.formatDate(new Date(), "yyyy-MM-dd"));
        gynetresModel.setId(UUID.randomUUID().toString().replace("-", ""));
        gynetresModel.setProcessTime(new Date());
        gynetresModel.setCreateTime(new Date());
        gynetresModel.setCounter(1L);
        gynetresModel.setPort("80");

        ParquetUtil.writeFile(Arrays.asList(gynetresModel, gynetresModel, gynetresModel, gynetresModel), messageType, "E:\\data\\dataprocessing\\util\\888.parquet", null);
    }

    @Test
    public void readFile() {
        String parquetPath = "E:\\data\\dataprocessing\\attack\\storage\\2d70d56862d5c425fae6c5fd994a9597.parquet";
        parquetPath = "E:\\data\\dataprocessing\\util\\88.parquet";
        parquetPath="E:\\data\\dataprocessing\\util\\merge.parquet";
        //parquetPath="E:\\data\\dataprocessing\\util\\35fcc9c144eb44c4af01c548fb741d23.parquet";
        //parquetPath="E:\\data\\dataprocessing\\util\\000000_0";
        //parquetPath = "E:\\data\\dataprocessing\\storage\\gynetres\\ds=2019-07-18\\cd10337fe44048e4930ffba83ec580d1.parquet";
        //parquetPath = "E:\\data\\02fcb929768e4fae9b8e4ebba975ca64_copy_45.parquet";
        //parquetPath = "E:\\data\\f79ca93b46d44cf98d86b6ebac92b96e.parquet";
        //parquetPath = "E:\\data\\dataprocessing\\attack\\hive\\ds=2019-07-18\\dbde3e5bebe544d6bc50d44d659c1761.parquet";
//        parquetPath = "E:\\data\\dataprocessing\\000000_0";
//        parquetPath = "E:\\data\\dataprocessing\\20190828185049_cad5bb14595f432e860ac949059e37e4.parquet";
//        parquetPath = "E:\\data\\dataprocessing\\964416bd2ed444628b4fae3136dbdb97.parquet";
//        parquetPath = "E:\\data\\dataprocessing\\c3424f3f48ba450ab3355392fc80f9e4.parquet";
        List<Map<String, Object>> maps = ParquetUtil.readFile(parquetPath);
        maps.forEach(map -> System.out.println(JSON.toJSONString(map, SerializerFeature.WriteDateUseDateFormat)));
    }

    @Test
    public void getInt96Value() {
        Date date = new Date();
        System.out.println(date.getTime());
        long timestampMillis = ParquetUtil.getTimestampMillis(ParquetUtil.getInt96Value(date));
        System.out.println(timestampMillis);
    }


    @Test
    public void mergeFiles() {
        List<File> files = new ArrayList<>();
        files.add(new File("E:\\data\\dataprocessing\\util\\35fcc9c144eb44c4af01c548fb741d23.parquet"));
        files.add(new File("E:\\data\\dataprocessing\\util\\52ece65b43e04e3388d430e63946867a.parquet"));
        files.add(new File("E:\\data\\dataprocessing\\util\\631fb46a2bcf4f1fbc75b7f339ebf27e.parquet"));
        files.add(new File("E:\\data\\dataprocessing\\util\\997e3df665da426d854e804756a78417.parquet"));
        ParquetUtil.mergeFiles(files, "E:\\data\\dataprocessing\\util\\merge.parquet", "", "snappy", 10000);
    }
}
