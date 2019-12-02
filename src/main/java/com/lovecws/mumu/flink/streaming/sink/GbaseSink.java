package com.lovecws.mumu.flink.streaming.sink;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.jdbc.GbaseJdbcService;
import com.lovecws.mumu.flink.common.jdbc.JdbcConfig;
import com.lovecws.mumu.flink.common.model.atd.AtdEventModel;
import com.lovecws.mumu.flink.common.model.atd.AtdPurgeModel;
import com.lovecws.mumu.flink.common.util.DateUtils;
import com.lovecws.mumu.flink.common.util.MD5Util;
import com.lovecws.mumu.flink.common.util.MapFieldUtil;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;

import java.util.*;

/**
 * @program: act-able
 * @description: atd清洗表存储
 * @author: 甘亮
 * @create: 2019-05-29 09:07
 **/
@Slf4j
@ModularAnnotation(type = "storage", name = "gbase")
public class GbaseSink extends JdbcSink {

    private Map<String, String> partitionInfo;

    public GbaseSink(Map<String, Object> configMap) {
        super(configMap);
        if (configMap == null) configMap = new HashMap<>();
        url = MapUtils.getString(configMap, "url", MapFieldUtil.getMapField(configMap, "spring.datasource.default.url").toString()).toString();
        driver = MapUtils.getString(configMap, "driver", MapFieldUtil.getMapField(configMap, "spring.datasource.default.driver-class-name").toString()).toString();
        user = MapUtils.getString(configMap, "user", MapFieldUtil.getMapField(configMap, "spring.datasource.default.username").toString()).toString();
        password = MapUtils.getString(configMap, "password", MapFieldUtil.getMapField(configMap, "spring.datasource.default.password").toString()).toString();

        if (StringUtils.isEmpty(url) || StringUtils.isEmpty(table))
            throw new IllegalArgumentException("illegal gbase url:" + url + " or table is empty");

        partitionInfo = new HashMap<>();
        partitionInfo.put("partitionField", MapUtils.getString(configMap, "partitionField", "").toString());
        partitionInfo.put("partitionType", MapUtils.getString(configMap, "partitionType", "").toString());
        partitionInfo.put("partitionRangeInterval", MapUtils.getString(configMap, "partitionRangeInterval", "").toString());
        partitionInfo.put("partitionRangeExpr", MapUtils.getString(configMap, "partitionRangeExpr", "").toString());
        partitionInfo.put("partitionHashCount", MapUtils.getString(configMap, "partitionHashCount", "").toString());
        partitionInfo.put("fieldTypeConvert", MapUtils.getString(configMap, "fieldTypeConvert", "").toString());
        partitionInfo.put("fieldNameConvert", MapUtils.getString(configMap, "fieldNameConvert", "").toString());
        partitionInfo.put("indexFields", MapUtils.getString(configMap, "indexFields", "").toString());
        partitionInfo.put("indexType", MapUtils.getString(configMap, "indexType", "").toString());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        jdbcConfig = new JdbcConfig(url, driver, user, password);
        jdbcConfig.setTableInfo(partitionInfo);
        jdbcService = new GbaseJdbcService(jdbcConfig);
    }

    @Override
    public void invoke(Object object, Context context) throws Exception {
        if (object == null) return;
        //上层传递的数据有可能是集合
        List<Object> datas = new ArrayList<>();
        if (object instanceof List) {
            datas.addAll((List) object);
        } else {
            datas.add(object);
        }

        List<Object> _data_ = new ArrayList<>();
        for (Object data : datas) {
            AtdEventModel atdEventModel = (AtdEventModel) data;
            AtdPurgeModel atdPurgeModel = JSON.parseObject(JSON.toJSONString(atdEventModel), AtdPurgeModel.class);
            atdPurgeModel.setBeginTime(atdEventModel.getTimestamp());
            atdPurgeModel.setStopTime(atdEventModel.getTimestamp());
            atdPurgeModel.setCreateTime(new Date());
            atdPurgeModel.setCount(1);
            atdPurgeModel.setId(MD5Util.md5(atdPurgeModel.generateKey()));
            String eventTypeId = atdPurgeModel.getEventTypeId();
            if (StringUtils.isEmpty(eventTypeId)) atdPurgeModel.setEventTypeId("0");

            String partitionRangeInterval = MapUtils.getString(jdbcConfig.getTableInfo(), "partitionRangeInterval", "day");
            atdPurgeModel.setDs(DateUtils.getDateByStragety(new Date(), partitionRangeInterval));
            _data_.add(atdPurgeModel);
        }
        super.invoke(_data_, context);
    }

    @Override
    public void handleData(List<Map<String, Object>> datas) {
        if (datas.size() > 1) log.info("gbaseSinkInvoke:{table:" + table + ",size:" + datas.size() + "}");
        jdbcService.batchUpset(table, tableInfo, datas);
    }

    @Override
    public void init(Object eventModel) {
        AtdPurgeModel atdPurgeModel = JSON.parseObject(JSON.toJSONString(eventModel), AtdPurgeModel.class);
        super.init(atdPurgeModel);

        //获取当前的分区表
        GbaseJdbcService gbaseJdbcService = (GbaseJdbcService) jdbcService;
        String currentPartitionTable = gbaseJdbcService.getCurrentPartitionTables(table, tableInfo);

        if (currentPartitionTable != null) {
            //获取数据库已经生成的分区表
            List<String> pgPartitionTables = gbaseJdbcService.getGbasePartitionTables(table);
            //判断当前的分区表是否在已经生成
            boolean flag = true;
            if (!pgPartitionTables.contains(currentPartitionTable)) flag = false;
            //分区表不存在 则创建分区表
            if (!flag) {
                String createTableSql = gbaseJdbcService.getAddPartitionSql(table, tableInfo);
                gbaseJdbcService.createTable(createTableSql);
            }
        }
    }
}
