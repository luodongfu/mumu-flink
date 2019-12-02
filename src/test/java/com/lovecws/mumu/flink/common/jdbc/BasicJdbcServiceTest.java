package com.lovecws.mumu.flink.common.jdbc;

import com.lovecws.mumu.flink.common.model.gynetres.GynetresModel;
import com.lovecws.mumu.flink.common.util.TableUtil;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @program: act-able
 * @description: 基础jdbc服务
 * @author: 甘亮
 * @create: 2019-06-14 11:04
 **/
public class BasicJdbcServiceTest {

    @Test
    public void clickHouse() {
        JdbcConfig jdbcConfig = new JdbcConfig("jdbc:clickhouse://172.31.134.225:8123/industry", "ru.yandex.clickhouse.ClickHouseDriver", "default", "lovecws");
        AbstractJdbcService jdbcService = new BasicJdbcService(jdbcConfig);
        GynetresModel gynetresModel = new GynetresModel();
        gynetresModel.setCreateTime(new Date());
        gynetresModel.setProcessTime(new Date());
        gynetresModel.setDs("2019-06-11");
        gynetresModel.setPort("80");
        Map<String, Object> tableInfo = TableUtil.getTableInfo(gynetresModel);
        List<Map<String, String>> tableFields = TableUtil.getTableFields(gynetresModel);

        String tableSql = jdbcService.getCreateTableSql("t_ods_industry_gynetres_test", tableInfo);
        jdbcService.createTable(tableSql);

        Map<String, Object> cloumnValues = TableUtil.getCloumnValues(gynetresModel, tableFields);
        //jdbcService.insertInto("t_ods_industry_gynetres_test", tableFields, cloumnValues);
        jdbcService.batchInsertInto("t_ods_industry_gynetres_test", tableFields, Arrays.asList(cloumnValues));
    }
}
