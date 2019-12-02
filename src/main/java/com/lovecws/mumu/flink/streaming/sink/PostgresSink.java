package com.lovecws.mumu.flink.streaming.sink;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.jdbc.JdbcConfig;
import com.lovecws.mumu.flink.common.jdbc.PostgreJdbcService;
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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @program: act-able
 * @description: postgresql清洗表存储
 * @author: 甘亮
 * @create: 2019-05-29 09:07
 **/
@Slf4j
@ModularAnnotation(type = "sink", name = "postgres")
public class PostgresSink extends JdbcSink {

    //过滤规则
    private String filter;

    public PostgresSink(Map<String, Object> configMap) {
        super(configMap);

        url = MapUtils.getString(configMap, "url", MapFieldUtil.getMapField(configMap, "spring.datasource.default.url").toString()).toString();
        driver = MapUtils.getString(configMap, "driver", MapFieldUtil.getMapField(configMap, "spring.datasource.default.driver-class-name").toString()).toString();
        user = MapUtils.getString(configMap, "user", MapFieldUtil.getMapField(configMap, "spring.datasource.default.username").toString()).toString();
        password = MapUtils.getString(configMap, "password", MapFieldUtil.getMapField(configMap, "spring.datasource.default.password").toString()).toString();

        if (StringUtils.isEmpty(url) || StringUtils.isEmpty(table))
            throw new IllegalArgumentException("illegal jdbc url:" + url + " or table is empty");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        jdbcConfig = new JdbcConfig(url, driver, user, password);
        jdbcService = new PostgreJdbcService(jdbcConfig);
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

            atdPurgeModel.setDs(DateUtils.formatDate(new Date(), "yyyyMMddHHmmss"));
            _data_.add(atdPurgeModel);
        }
        super.invoke(_data_, context);
    }

    @Override
    public void handleData(List<Map<String, Object>> datas) {
        if (datas.size() > 1) log.info("postgresSinkInvoke:{table:" + table + ",size:" + datas.size() + "}");
        jdbcService.batchUpset(table, tableInfo, datas);
    }

    @Override
    public void init(Object eventModel) {
        //设置时间
        jdbcConfig.setCurrentDate(new Date());

        AtdPurgeModel atdPurgeModel = JSON.parseObject(JSON.toJSONString(eventModel), AtdPurgeModel.class);
        super.init(atdPurgeModel);

        //获取当前的分区表
        PostgreJdbcService postgreJdbcService = (PostgreJdbcService) jdbcService;
        List<String> currentPartitionTables = postgreJdbcService.getCurrentPartitionTables(table, tableInfo);

        if (currentPartitionTables.size() > 0) {
            //获取数据库已经生成的分区表
            List<String> pgPartitionTables = postgreJdbcService.getPgPartitionTables(table);
            //判断当前的分区表是否在已经生成
            boolean flag = true;
            for (String partitionTable : currentPartitionTables) {
                if (!pgPartitionTables.contains(partitionTable)) {
                    flag = false;
                    break;
                }
            }
            //分区表不存在 则创建分区表
            if (!flag) {
                String createTableSql = jdbcService.getCreateTableSql(table, tableInfo);
                jdbcService.createTable(createTableSql);
            }
        }
    }
}
