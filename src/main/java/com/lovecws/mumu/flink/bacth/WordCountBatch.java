package com.lovecws.mumu.flink.bacth;

import com.lovecws.mumu.flink.MumuFlinkConfiguration;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.io.SerializedInputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.AvroFSInput;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: flink batch
 * @date 2018-02-28 14:28
 */
public class WordCountBatch {

    private void wordCount(DataSet<String> dataSet) throws Exception {
        FlatMapOperator<String, Tuple2<String, Integer>> mapDataSet = dataSet.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(final String line, final Collector<Tuple2<String, Integer>> collector) throws Exception {
                System.out.println(line);
                for (String word : line.split("\\s+")) {
                    if (!word.isEmpty()) {
                        collector.collect(new Tuple2<>(word, 1));
                    }
                }
            }
        });
        AggregateOperator<Tuple2<String, Integer>> aggregateOperator = mapDataSet.groupBy(0).sum(1);
        aggregateOperator.print();
    }

    public void fromElements(String... elements) throws Exception {
        ExecutionEnvironment executionEnvironment = MumuFlinkConfiguration.executionEnvironment();
        DataSet<String> dataSet = executionEnvironment.fromElements(elements);
        wordCount(dataSet);
    }

    public void textFile(String filePath) throws Exception {
        ExecutionEnvironment executionEnvironment = MumuFlinkConfiguration.executionEnvironment();
        // create a configuration object
        Configuration parameters = new Configuration();

        // set the recursive enumeration parameter
        parameters.setBoolean("recursive.file.enumeration", true);
        DataSet<String> dataSet = executionEnvironment.readTextFile(filePath).withParameters(parameters);
        wordCount(dataSet);
    }
}
