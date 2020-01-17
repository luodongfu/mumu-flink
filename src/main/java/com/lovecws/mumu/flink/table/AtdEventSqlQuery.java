package com.lovecws.mumu.flink.table;

import com.alibaba.fastjson.JSON;
import com.lovecws.mumu.flink.common.model.atd.AtdEventModel;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.util.Map;

/**
 * @program: mumu-flink
 * @description: atdEvent sql查询
 * @author: 甘亮
 * @create: 2019-12-02 13:36
 **/
public class AtdEventSqlQuery {

    public void sqlQuery(String filePath, String outPath) {

        try {
            ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
            BatchTableEnvironment tableEnvironment = BatchTableEnvironment.create(executionEnvironment);

            Configuration parameters = new Configuration();
            parameters.setBoolean("recursive.file.enumeration", true);

            DataSource<String> stringDataSource = executionEnvironment.readTextFile(filePath).withParameters(parameters);

            MapOperator<String, Tuple2<String, String>> mapMapOperator = stringDataSource.map(new MapFunction<String, Tuple2<String, String>>() {
                @Override
                public Tuple2<String, String> map(String value) throws Exception {
                    AtdEventModel atdEventModel = JSON.parseObject(value, AtdEventModel.class);
                    return new Tuple2<String, String>(atdEventModel.getSrcIp(), atdEventModel.getSrcPort());
                }
            }).returns(TypeInformation.of(new TypeHint<Tuple2<String, String>>() {
            }));

            tableEnvironment.registerDataSet("t_ods_industry_atd", mapMapOperator, "srcIp,srcPort");
            Table table = tableEnvironment.sqlQuery("select srcIp,srcPort,count(1) as counter from t_ods_industry_atd group by srcIp,srcPort order by counter desc limit 10");
            DataSet<Tuple3<String, String, Long>> result = tableEnvironment.toDataSet(table, TypeInformation.of(new TypeHint<Tuple3<String, String, Long>>() {
            }));
            result.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void atdSqlQuery(String filePath) {
        try {
            ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
            BatchTableEnvironment tableEnvironment = BatchTableEnvironment.create(executionEnvironment);

            Configuration parameters = new Configuration();
            parameters.setBoolean("recursive.file.enumeration", true);

            DataSource<String> stringDataSource = executionEnvironment.readTextFile(filePath).withParameters(parameters);

            MapOperator<String, Map<String, Object>> mapMapOperator = stringDataSource.map(new MapFunction<String, Map<String, Object>>() {
                @Override
                public Map<String, Object> map(String value) throws Exception {
                    return JSON.parseObject(value, Map.class);
                }
            }).returns(TypeInformation.of(new TypeHint<Map<String, Object>>() {
            }));

            tableEnvironment.registerDataSet("t_ods_industry_atd", mapMapOperator);
            Table table = tableEnvironment.sqlQuery("select srcIp,srcPort,count(1) as counter from t_ods_industry_atd group by srcIp,srcPort order by counter desc limit 10");
            DataSet<Tuple3<String, String, Long>> result = tableEnvironment.toDataSet(table, TypeInformation.of(new TypeHint<Tuple3<String, String, Long>>() {
            }));

            result.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void atdTableQuery(String filePath) {
        try {
            ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
            BatchTableEnvironment tableEnvironment = BatchTableEnvironment.create(executionEnvironment);

            tableEnvironment
                    .connect(new FileSystem().path(filePath))
                    .withFormat(new Json()
                            .failOnMissingField(false)
                            .deriveSchema())
                    .withSchema(new Schema()
                            .field("category", Types.STRING())
                            .field("family", Types.STRING())
                    )
                    .registerTableSource("t_ods_industry_atd");

            Table table = tableEnvironment.scan("t_ods_industry_atd");
            tableEnvironment.toDataSet(table, Row.class).print();

            executionEnvironment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
