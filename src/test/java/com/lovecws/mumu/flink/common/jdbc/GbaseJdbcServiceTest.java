package com.lovecws.mumu.flink.common.jdbc;

import com.lovecws.mumu.flink.common.model.atd.AtdPurgeModel;
import com.lovecws.mumu.flink.common.model.gynetres.GynetresModel;
import com.lovecws.mumu.flink.common.util.TableUtil;
import org.junit.Test;

import java.util.*;

/**
 * @program: mumu-flink
 * @description: 基础jdbc服务
 * @author: 甘亮
 * @create: 2019-06-14 11:04
 **/
public class GbaseJdbcServiceTest {

    private static JdbcConfig jdbcConfig = null;

    static {
        Map<String, String> partitionInfo = new HashMap<>();
        partitionInfo.put("partitionField", "ds,event_type_id");
        partitionInfo.put("partitionType", "range,hash");
        partitionInfo.put("partitionRangeInterval", "week");
        partitionInfo.put("partitionRangeExpr", "MICROSECOND");
        partitionInfo.put("partitionHashCount", "18");
        partitionInfo.put("indexFields", "id,ds,event_type_id");
        partitionInfo.put("indexType", "primarykey");
        jdbcConfig = new JdbcConfig("jdbc:gbase://172.31.134.249:5258/test", "com.gbase.jdbc.Driver", "sysdba", "GBase8sV8316");
        jdbcConfig.setTableInfo(partitionInfo);
    }

    @Test
    public void gbaseGynetres() {
        AbstractJdbcService jdbcService = new BasicJdbcService(jdbcConfig);
        GynetresModel gynetresModel = new GynetresModel();
        gynetresModel.setCreateTime(new Date());
        gynetresModel.setProcessTime(new Date());
        gynetresModel.setDs("2019-06-11");
        gynetresModel.setPort("80");
        Map<String, Object> tableInfo = TableUtil.getTableInfo(gynetresModel);


        String tableSql = jdbcService.getCreateTableSql("t_ods_industry_gynetres_test", tableInfo);
        jdbcService.createTable(tableSql);
    }

    @Test
    public void gbaseAtd() {
        AbstractJdbcService jdbcService = new GbaseJdbcService(jdbcConfig);
        AtdPurgeModel atdEventModel = new AtdPurgeModel();
        atdEventModel.setCreateTime(new Date());
        atdEventModel.setDs("2019-06-11");
        atdEventModel.setEventTypeId("1");
        atdEventModel.setId("1");
        atdEventModel.setCount(1);
        Map<String, Object> tableInfo = TableUtil.getTableInfo(atdEventModel);

        String tableSql = jdbcService.getCreateTableSql("t_ods_industry_atd_test", tableInfo);
        System.out.println(tableSql);
        jdbcService.createTable(tableSql);

        String addPartitionSql = ((GbaseJdbcService) jdbcService).getAddPartitionSql("t_ods_industry_atd_test", tableInfo);
        System.out.println(addPartitionSql);

        List<Map<String, String>> tableFields = TableUtil.getTableFields(atdEventModel);
        Map<String, Object> cloumnValues = TableUtil.getCloumnValues(atdEventModel, tableFields);
        jdbcService.batchInsertInto("t_ods_industry_atd_test", tableFields, Arrays.asList(cloumnValues));
    }

    @Test
    public void gbaseUpset() {
        GbaseJdbcService jdbcService = new GbaseJdbcService(jdbcConfig);
        AtdPurgeModel atdEventModel = new AtdPurgeModel();
        atdEventModel.setCreateTime(new Date());
        atdEventModel.setDs("2019-06-11");
        atdEventModel.setEventTypeId("1");
        atdEventModel.setId("1");
        atdEventModel.setCount(1);
        Map<String, Object> tableInfo = TableUtil.getTableInfo(atdEventModel);

        Map<String, Object> cloumnValues = TableUtil.getCloumnValues(atdEventModel, TableUtil.getTableFields(atdEventModel));

        jdbcService.batchUpset("t_ods_industry_atd_test", tableInfo, Arrays.asList(cloumnValues));
    }

    @Test
    public void tableExists() {
        AbstractJdbcService jdbcService = new BasicJdbcService(jdbcConfig);
        boolean tableExists = jdbcService.tableExists("t_ods_industry_atd");
        System.out.println(tableExists);
    }

}
