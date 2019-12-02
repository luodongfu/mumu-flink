package com.lovecws.mumu.flink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.BatchTableEnvironment;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: apache flink configuration
 * @date 2018-02-27 16:01
 */
public class MumuFlinkConfiguration {

    private static String FLINK_ENVIRONMENT_HOST = "172.31.134.225";
    private static int FLINK_ENVIRONMENT_PORT = 6123; //jobmanager.rpc.port

    public static ExecutionEnvironment executionEnvironment() {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.minutes(10)));
        return environment;
    }

    public static ExecutionEnvironment createRemoteEnvironment() {
        return ExecutionEnvironment.createRemoteEnvironment(FLINK_ENVIRONMENT_HOST, FLINK_ENVIRONMENT_PORT);
    }

    public static StreamExecutionEnvironment streamExecutionEnvironment() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

    public static BatchTableEnvironment batchTableEnvironment() {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.createCollectionsEnvironment();
        return TableEnvironment.getTableEnvironment(executionEnvironment);
    }
}
