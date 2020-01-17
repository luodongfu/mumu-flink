package com.lovecws.mumu.flink.streaming.storage;

import com.lovecws.mumu.flink.common.model.atd.AtdEventModel;
import com.lovecws.mumu.flink.streaming.sink.PostgresSink;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @program: mumu-flink
 * @description: atd清洗表存储测试
 * @author: 甘亮
 * @create: 2019-06-12 15:29
 **/
public class AtdPurgeDataStorageTest {

    @Test
    public void storage() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("url", "jdbc:postgresql://172.31.134.225:5432/ads");
        configMap.put("driver", "org.postgresql.Driver");
        configMap.put("user", "ads");
        configMap.put("password", "ads@123");
        configMap.put("table", "t_ods_industry_atd");
        PostgresSink postgresSink = new PostgresSink(configMap);

        AtdEventModel atdEventModel = new AtdEventModel();
        atdEventModel.setTimestamp(new Date());
        atdEventModel.setEventTypeId("6");

        postgresSink.invoke(atdEventModel,null);
    }
}
