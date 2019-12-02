package com.lovecws.mumu.flink.table;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;

import java.util.Map;

/**
 * @program: mumu-flink
 * @description: ${description}
 * @author: 甘亮
 * @create: 2019-12-02 13:36
 **/
public class Sql {

    public void sqlQuery(){
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.createCollectionsEnvironment();
        BatchTableEnvironment tableEnvironment = TableEnvironment.getTableEnvironment(executionEnvironment);

        DataSource<String> stringDataSource = executionEnvironment.readTextFile("E:\\data\\mumuflink\\atd\\localfile\\2019112109");
        MapOperator<String, Map<String, Object>> mapMapOperator = stringDataSource.map(new MapFunction<String, Map<String, Object>>() {
            @Override
            public Map<String, Object> map(String value) throws Exception {
                System.out.println(value);
                return JSON.parseObject(value, Map.class);
            }
        });
        tableEnvironment.registerDataSet("t_ods_industry_atd", mapMapOperator);
        Table table = tableEnvironment.sqlQuery("select count(1) from t_ods_industry_atd");

        TableSink csvSink = new CsvTableSink("E:\\data\\mumuflink", "|");
        String[] fieldNames = {"count"};
        TypeInformation[] fieldTypes = {Types.LONG};
        tableEnvironment.registerTableSink("RubberOrders", fieldNames, fieldTypes, csvSink);

        table.insertInto("RubberOrders");
    }
}
