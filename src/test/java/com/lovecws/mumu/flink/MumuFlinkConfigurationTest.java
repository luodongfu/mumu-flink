package com.lovecws.mumu.flink;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.SortPartitionOperator;
import org.junit.Test;

import java.util.Arrays;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: flink配置测试
 * @date 2018-02-28 11:44
 */
public class MumuFlinkConfigurationTest {

    public static final MumuFlinkConfiguration flinkConfiguration = new MumuFlinkConfiguration();

    @Test
    public void createRemoteEnvironment() throws Exception {
        ExecutionEnvironment remoteEnvironment = flinkConfiguration.createRemoteEnvironment();
        DataSource<String> stringDataSource = remoteEnvironment.fromCollection(Arrays.asList("12", "2", "10", "14"));
        stringDataSource.print();
    }
}
