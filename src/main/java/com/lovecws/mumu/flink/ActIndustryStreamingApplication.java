package com.lovecws.mumu.flink;

import com.lovecws.mumu.flink.common.config.ConfigProperties;
import com.lovecws.mumu.flink.streaming.config.ConfigManager;
import com.lovecws.mumu.flink.streaming.task.AbstractStreamingTask;
import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @program: act-industry-data-streaming
 * @description: 数据流处理程序
 * @author: 甘亮
 * @create: 2019-11-06 19:13
 **/
public class ActIndustryStreamingApplication {

    private static void executeTask(String task) {
        ConfigManager configManager = new ConfigManager();
        AbstractStreamingTask streamingTask = configManager.getStreamingTask(task);
        streamingTask.streaming();
    }

    private static void executeTasks(String[] tasks) {
        if (tasks.length == 1) {
            executeTask(tasks[0]);
        } else {
            ExecutorService executorService = Executors.newFixedThreadPool(tasks.length);
            for (String task : tasks) {
                executorService.execute(new Runnable() {
                    @Override
                    public void run() {
                        executeTask(task);
                    }
                });
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("es.set.netty.runtime.available.processors", "false");
        //如果没有设置参数 则从配置文件拉取所有的任务
        if (args == null || args.length == 0 || StringUtils.isEmpty(args[0])) {
            String tasks = ConfigProperties.getString("streaming.tasks.active");
            if (StringUtils.isEmpty(tasks)) throw new IllegalArgumentException();
            executeTasks(tasks.split(","));
        }
        //从命令行读取执行的任务
        else {
            executeTasks(args);
        }
    }
}
