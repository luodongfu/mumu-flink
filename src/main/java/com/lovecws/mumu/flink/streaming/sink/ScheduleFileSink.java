package com.lovecws.mumu.flink.streaming.sink;

import com.lovecws.mumu.flink.common.util.DateUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @program: trunk
 * @description: 文件定时保存ftp、sftp、hdfs
 * @author: 甘亮
 * @create: 2019-11-05 17:04
 **/
@Slf4j
public abstract class ScheduleFileSink extends LocalFileSink {

    private long loadDataInterval;

    public ScheduleFileSink(Map<String, Object> configMap) {
        super(configMap);
        loadDataInterval = DateUtils.getExpireTime(MapUtils.getString(configMap, loadDataInterval, "60s"));
    }

    public abstract void executeScheduleTask(List<File> files);


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        startScheduleTask();
    }

    private static final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    /**
     * 开启线程 定时将文件上传到sftp目录下
     */
    private void startScheduleTask() {
        //当加载时间小于等于0的时候 不开启线程定时处理
        if (loadDataInterval <= 0) return;
        scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                log.info("startScheduleTask [path:" + path + "] [loadDataInterval:" + loadDataInterval + "]");
                try {
                    //如果扫描文件目录不存在 则退出当前扫描
                    File pathDirectory = new File(path);
                    if (!pathDirectory.exists()) {
                        return;
                    }
                    Collection<File> files = FileUtils.listFiles(pathDirectory, null, true);
                    List<File> fileList = new ArrayList<>();
                    for (File file : files) {
                        if (file.isHidden()) continue;
                        //文件最后修改时间小于扫描间隔时间 则判断是否存在inprogress文件
                        if (System.currentTimeMillis() - file.lastModified() < Math.max(loadDataInterval, 3600 * 24 * 7) * 1000) {
                            if (file.getName().startsWith(".")) continue;
                            if (file.getName().contains(".inprogress.")) continue;
                        }
                        fileList.add(file);
                    }
                    executeScheduleTask(fileList);
                } catch (Exception ex) {
                    log.error(ex.getLocalizedMessage(), ex);
                }
            }
        }, 10, loadDataInterval, TimeUnit.SECONDS);
    }
}
