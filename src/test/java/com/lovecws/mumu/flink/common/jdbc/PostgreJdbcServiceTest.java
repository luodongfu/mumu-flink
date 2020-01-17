package com.lovecws.mumu.flink.common.jdbc;

import com.lovecws.mumu.flink.common.model.atd.AtdPurgeModel;
import com.lovecws.mumu.flink.common.util.DateUtils;
import com.lovecws.mumu.flink.common.util.TableUtil;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @program: mumu-flink
 * @description: Postgre服务测试
 * @author: 甘亮
 * @create: 2019-06-12 14:17
 **/
public class PostgreJdbcServiceTest {

    @Test
    public void getCreateTableSql() {
        JdbcConfig jdbcConfig = new JdbcConfig("jdbc:hive2://localhost:10001/ads", "org.apache.hive.jdbc.HiveDriver", "admin", "");
        PostgreJdbcService postgreJdbcService = new PostgreJdbcService(jdbcConfig);
        String tableSql = postgreJdbcService.getCreateTableSql("t_ods_industry_atd", TableUtil.getTableInfo(new AtdPurgeModel()));
        System.out.println(tableSql);
    }

    @Test
    public void tables() {
        int weekCount = 40;
        String stragety = "week";
        String[] events = new String[]{"1:malicious_software", "2:loophole_scan", "3:bot_net", "4:web_injection",
                "5:extortion_virus", "6:mining_events", "7:command_execution", "8:trojan_backdoor", "9:brute_force", "10:spam_fraud", "11:dos_attack",
                "12:auth_infiltration", "13:intelligence_outreach",
                "14:loophole_attack", "15:abnormal_traffic", "16:apt_attack"};

        String primaryTable = "CREATE TABLE IF NOT EXISTS t_ods_industry_atd ( id text,event_type_id text,ds text,src_ip text,src_port text,src_corp_name text,src_industry text,src_ip_country text,src_ip_province text,src_ip_city text,dst_ip text,dst_port text,corp_name text,industry text,dst_ip_country text,dst_ip_province text,dst_ip_city text,category text,kill_chain text,severity_cn text,severity int,atd_desc text,conn_src_mac text,conn_duration text,conn_dst_mac text,payload text,family text,dns_query text,count int,begin_time timestamp,stop_time timestamp,create_time timestamp,proto text,app_proto text,http_host text,attack_ip text,attack_port text,attack_country text,attack_province text,attack_city text,attack_corpname text,attack_industry text,attacked_ip text,attacked_port text,attacked_country text,attacked_province text,attacked_city text,attacked_corpname text,attacked_industry text) PARTITION BY range(event_type_id,ds);\n";

        //创建分区表
        List<String> tables = new ArrayList<>();
        StringBuffer partitionTableBuffer = new StringBuffer("\n\n");

        for (int i = 0; i < weekCount; i++) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(new Date());
            calendar.add(Calendar.WEEK_OF_MONTH, i);
            String tableDate = DateUtils.getPartitionTable(calendar.getTime(), stragety);

            //本周周一时间
            String startDay = tableDate + "000000";
            //本周周六时间
            String endDay = DateUtils.getPartitionTableEnding(calendar.getTime(), stragety);
            for (String event : events) {
                String[] splits = event.split(":");
                String eventTypeId = splits[0];
                String eventName = splits[1];
                String tableName = "t_ods_industry_atd_" + eventName + "_" + tableDate;
                tables.add(tableName);
                partitionTableBuffer.append("CREATE TABLE IF NOT EXISTS " + tableName + " PARTITION OF t_ods_industry_atd FOR VALUES FROM (" + eventTypeId + "," + startDay + ") TO (" + eventTypeId + "," + endDay + ");\n");
            }
            partitionTableBuffer.append("\n");
        }
        //索引
        StringBuffer indexBuffer = new StringBuffer("\n\n");
        tables.forEach(tableName -> {
            indexBuffer.append("ALTER TABLE " + tableName + " DROP CONSTRAINT IF EXISTS " + tableName + "_pk;\n");
            indexBuffer.append("ALTER TABLE " + tableName + " ADD  CONSTRAINT " + tableName + "_pk PRIMARY KEY(id);\n");
        });

        String createTable = primaryTable + partitionTableBuffer.toString() + indexBuffer.toString();
        System.out.println(createTable);
    }

    @Test
    public void getCurrentPartitionTables() {
        JdbcConfig jdbcConfig = new JdbcConfig("jdbc:hive2://localhost:10001/ads", "org.apache.hive.jdbc.HiveDriver", "admin", "");
        PostgreJdbcService postgreJdbcService = new PostgreJdbcService(jdbcConfig);
        List<String> tables = postgreJdbcService.getCurrentPartitionTables("t_ods_industry_atd", TableUtil.getTableInfo(new AtdPurgeModel()));
        tables.forEach(table -> System.out.println(table));
    }

    @Test
    public void tableExists() {
        JdbcConfig jdbcConfig = new JdbcConfig("jdbc:postgresql://172.31.134.225:5432/ableads", "org.postgresql.Driver", "postgres", "postgres");
        AbstractJdbcService jdbcService = new PostgreJdbcService(jdbcConfig);

        boolean tableExists = jdbcService.tableExists("t_ods_industry_atd");
        System.out.println(tableExists);
    }

    @Test
    public void databaseExists() {
        JdbcConfig jdbcConfig = new JdbcConfig("jdbc:postgresql://172.31.134.225:5432/ableads", "org.postgresql.Driver", "postgres", "postgres");
        AbstractJdbcService jdbcService = new PostgreJdbcService(jdbcConfig);

        boolean databaseExists = jdbcService.databaseExists("test2");
        System.out.println(databaseExists);
    }

    @Test
    public void createDatabase() {
        JdbcConfig jdbcConfig = new JdbcConfig("jdbc:postgresql://172.31.134.225:5432/ableads", "org.postgresql.Driver", "postgres", "postgres");
        AbstractJdbcService jdbcService = new PostgreJdbcService(jdbcConfig);
        boolean databaseExists = jdbcService.createDatabase("test3");
        System.out.println(databaseExists);
    }
}
