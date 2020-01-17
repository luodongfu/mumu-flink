package com.lovecws.mumu.flink.common.jdbc;

import com.lovecws.mumu.flink.common.model.attack.AttackEventModel;
import com.lovecws.mumu.flink.common.model.attack.FlowEventModel;
import com.lovecws.mumu.flink.common.model.gynetres.GynetresModel;
import com.lovecws.mumu.flink.common.util.DateUtils;
import com.lovecws.mumu.flink.common.util.TableUtil;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @program: mumu-flink
 * @description: hive的jdbc服务测试
 * @author: 甘亮
 * @create: 2019-06-11 11:31
 **/
public class HiveJdbcServiceTest {

    private HiveJdbcService hiveJdbcService;

    @Before
    public void before() {
        JdbcConfig jdbcConfig = new JdbcConfig("jdbc:hive2://172.31.134.216:10000/test", "org.apache.hive.jdbc.HiveDriver", "admin", "", "UTF-8");
        hiveJdbcService = new HiveJdbcService(jdbcConfig);
    }

    @Test
    public void gynetresInsertInto() {
        GynetresModel gynetresModel = new GynetresModel();
        gynetresModel.setCreateTime(new Date());
        gynetresModel.setProcessTime(new Date());
        gynetresModel.setDs("2019-06-11");
        List<Map<String, String>> tableFields = TableUtil.getTableFields(gynetresModel);

        Map<String, Object> cloumnValues = TableUtil.getCloumnValues(gynetresModel, tableFields);
        hiveJdbcService.insertInto("t_ods_industry_gynetres_test", tableFields, cloumnValues);
    }

    @Test
    public void attackInsertInto() {
        AttackEventModel attackEventModel = new AttackEventModel();
        attackEventModel.setCreateTime(new Date());
        attackEventModel.setAttackLevel(1);
        attackEventModel.setSourceIP("172.31.134.225");
        attackEventModel.setSourcePort(80);
        attackEventModel.setDestIP("58.20.16.24");
        attackEventModel.setDestPort(443);
        attackEventModel.setSourceCompany("湖南天汽模汽车模具技术股份有限公司");
        attackEventModel.setDestCompany("中国移动通信集团湖南有限公司");
        attackEventModel.setDs(DateUtils.formatDate(attackEventModel.getCreateTime(), "yyyy-MM-dd"));
        List<Map<String, String>> tableFields = TableUtil.getTableFields(attackEventModel);

        Map<String, Object> cloumnValues = TableUtil.getCloumnValues(attackEventModel, tableFields);
        hiveJdbcService.insertInto("t_ods_industry_attack_test", tableFields, cloumnValues);
    }

    @Test
    public void attackCreateTable() {
        Map<String, Object> tableInfo = TableUtil.getTableInfo(new AttackEventModel());
        boolean success = hiveJdbcService.createTable(hiveJdbcService.getCreateTableSql("t_ods_industry_attack_test", tableInfo));
        System.out.println(success);
    }

    @Test
    public void attackInsertIntoLocalFile() {
        AttackEventModel attackEventModel = new AttackEventModel();
        attackEventModel.setCreateTime(new Date());
        attackEventModel.setAttackLevel(1);
        attackEventModel.setSourceIP("172.31.134.225");
        attackEventModel.setSourcePort(80);
        attackEventModel.setDestIP("58.20.16.24");
        attackEventModel.setDestPort(443);
        attackEventModel.setSourceCompany("湖南天汽模汽车模具技术股份有限公司");
        attackEventModel.setDestCompany("中国移动通信集团湖南有限公司");
        attackEventModel.setDs(DateFormatUtils.format(new Date(), "yyyy-MM-dd"));
        List<Map<String, String>> tableFields = TableUtil.getTableFields(attackEventModel);

        Map<String, Object> cloumnValues = TableUtil.getCloumnValues(attackEventModel, tableFields);

        boolean success = hiveJdbcService.insertInto("t_ods_industry_attack_test", tableFields, cloumnValues);
        //success = hiveJdbcService.insertIntoLocalFile(tableFields, cloumnValues, "E:\\data\\dataprocessing\\attack\\storage", "parquet", "snappy", -1, -1);
        System.out.println(success);
    }

    @Test
    public void attackLoadDataLocalInpath() {
        hiveJdbcService.loadDataLocalInpath("t_ods_industry_attack_test_parquet", "E:\\data\\dataprocessing\\attack\\storage", "", "", -1, -1);
    }

    @Test
    public void getCreateTableSql() {
        Map<String, Object> tableInfo = TableUtil.getTableInfo(new FlowEventModel());
        String tableSql = hiveJdbcService.getCreateTableSql("t_ods_industry_anjia_flow_1028", tableInfo);
        System.out.println(tableSql);
    }

    @Test
    public void tableExists() {
        boolean tableExists = hiveJdbcService.tableExists("t_ods_industry_atd");
        System.out.println(tableExists);
    }

    @Test
    public void databaseExists() {
        boolean databaseExists = hiveJdbcService.databaseExists("test2");
        System.out.println(databaseExists);
    }

    @Test
    public void createDatabase() {
        boolean databaseExists = hiveJdbcService.createDatabase("test3");
        System.out.println(databaseExists);
    }
}
