package com.lovecws.mumu.flink.streaming.sink;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.jdbc.HiveJdbcService;
import com.lovecws.mumu.flink.common.jdbc.JdbcConfig;
import com.lovecws.mumu.flink.common.util.MapFieldUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * hive存储
 */
@Slf4j
@ModularAnnotation(type = "sink", name = "hive")
public class HiveSink extends JdbcSink {

    private String insertType;//数据插入方式 insertinto  loaddata
    private String dataPath;//本地文件路径
    private String compressionCodec;//压缩编码

    private int timeCount;//hive存储多久时间生成一个文件
    private int lineCount;//当文件数量超过多少行，之后切换一个文件存储
    private String hdfsPath;//hdfs文件路径，如果配置hdfs路径 则本地先将文件合并保存到hdfs上，然后再从hdfs上加载数据到hive
    private int loadDataInterval;//hive多久将本地存储的文件加载到hive表上

    private static final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    //loaddata线程多线程控制
    private static final Map<String, Boolean> loadDataTableMap = new ConcurrentHashMap<>();

    public HiveSink(Map<String, Object> configMap) {
        super(configMap);
        if (configMap == null) {
            configMap = new HashMap<>();
        }
        url = MapUtils.getString(configMap, "url", MapFieldUtil.getMapField(configMap, "defaults.hive.url").toString());
        driver = MapUtils.getString(configMap, "driver", MapFieldUtil.getMapField(configMap, "defaults.hive.driver").toString());
        user = MapUtils.getString(configMap, "user", MapFieldUtil.getMapField(configMap, "defaults.hive.user").toString());
        password = MapUtils.getString(configMap, "password", MapFieldUtil.getMapField(configMap, "defaults.hive.password").toString());
        charset = MapUtils.getString(configMap, "charset", MapFieldUtil.getMapField(configMap, "defaults.hive.charset").toString());

        if (StringUtils.isEmpty(url) || StringUtils.isEmpty(table)) {
            throw new IllegalArgumentException("illegal hive url:" + url + " or table is empty");
        }

        insertType = MapUtils.getString(configMap, "insertType", "insertinto");

        dataPath = MapUtils.getString(configMap, "dataPath", "/tmp/industrystreaming/hivesink").toString();
        hdfsPath = MapUtils.getString(configMap, "hdfsPath", "").toString();
        if ("loaddata".equalsIgnoreCase(insertType) && StringUtils.isEmpty(hdfsPath))
            throw new IllegalArgumentException("hive loaddata hdfsPath is empty!");

        compressionCodec = MapUtils.getString(configMap, "compressionCodec", "SNAPPY").toString();

        timeCount = MapUtils.getIntValue(configMap, "timeCount", -1);
        lineCount = MapUtils.getIntValue(configMap, "lineCount", -1);
        loadDataInterval = MapUtils.getIntValue(configMap, "loadDataInterval", 300);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        jdbcConfig = new JdbcConfig(url, driver, user, password, charset);
        jdbcService = new HiveJdbcService(jdbcConfig);
        startLoadDataTask();
    }

    @Override
    public void invoke(Object object, Context context) throws Exception {
        super.invoke(object, context);
    }

    @Override
    public void handleData(List<Map<String, Object>> datas) {
        if (datas.size() > 1)
            log.info("hiveSinkInvoke:{table:" + table + ",insertType:" + insertType + ",dataPath:" + dataPath + ",size:" + datas.size() + "}");
        if ("INSERTINTO".equalsIgnoreCase(insertType)) {
            jdbcService.batchInsertInto(table, columns, datas);
        } else if ("LOADDATA".equalsIgnoreCase(insertType)) {
            ((HiveJdbcService) jdbcService).batchInsertIntoLocalFile(columns, datas, dataPath, storage, compressionCodec, timeCount, lineCount);
        }
    }

    private void startLoadDataTask() {
        //如果使用loaddata保存hive数据 需要开启一个线程专门处理文件上传的业务逻辑
        if ("LOADDATA".equalsIgnoreCase(insertType)) {
            Boolean loadDataThreadExists = loadDataTableMap.putIfAbsent(table, true);
            //当多线程执行任务的时候 只需要开启一个loadData数据线程
            if (loadDataThreadExists != null && loadDataThreadExists) {
                return;
            }

            scheduledExecutor.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    log.info("startLoadDataTask [" + table + "] [" + dataPath + "] [" + hdfsPath + "] [" + timeCount + "s]");
                    try {
                        ((HiveJdbcService) jdbcService).loadDataLocalInpath(table, dataPath, hdfsPath, compressionCodec, timeCount <= 0 ? 10 : timeCount, lineCount);
                    } catch (Exception ex) {
                        log.error(ex.getLocalizedMessage(), ex);
                    }
                }
            }, 10, loadDataInterval, TimeUnit.SECONDS);
        }
    }
}
