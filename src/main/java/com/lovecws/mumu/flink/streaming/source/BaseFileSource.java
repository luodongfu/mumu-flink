package com.lovecws.mumu.flink.streaming.source;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.redis.JedisUtil;
import com.lovecws.mumu.flink.common.util.AvroUtil;
import com.lovecws.mumu.flink.common.util.MD5Util;
import com.lovecws.mumu.flink.common.util.ParquetUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import redis.clients.jedis.JedisCommands;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.StringReader;
import java.util.*;

/**
 * @program: act-able
 * @description: 基本文件数据源, 从文件读取数据(csv 、 json 、 avro 、 parquet 、 txt等)。读取文件的时候会将文件信息记录到状态信息中。恢复的时候可以继续读取文件。
 * @author: 甘亮
 * @create: 2019-11-21 17:51
 **/
@Slf4j
@ModularAnnotation(type = "source", name = "basefile")
public class BaseFileSource extends AbstractRichFunction implements BaseSource, SourceFunction<Object>, CheckpointedFunction {

    //文件根目录
    private String path;
    //文件类型
    private String fileType;
    //每次收集的文件数量
    private int batchCount;

    //csv格式 字段集合,字段分隔符,是否包含字段头
    private String[] csvHeaders;
    private String csvSeparator;
    private boolean hasCsvHeader;

    //过滤文件规则 目前支持checkpoint、redis、delete测试
    public String filterMode;
    public String filterRedisPrefix;

    public BaseFileSource(Map<String, Object> configMap) {
        path = MapUtils.getString(configMap, "path", "");
        if (StringUtils.isEmpty(path)) throw new IllegalArgumentException("path is not all null!");

        filterMode = MapUtils.getString(configMap, "filterMode", "checkpoint").toLowerCase();
        filterRedisPrefix = MapUtils.getString(configMap, "filterRedisPrefix", "STREAMING_BASEFILESOURCE_FILEFILTER").toLowerCase();

        fileType = MapUtils.getString(configMap, "fileType", "json").toLowerCase();
        batchCount = MapUtils.getIntValue(configMap, "batchCount", 1);

        //csv格式类型附加字段信息
        csvHeaders = MapUtils.getString(configMap, "csvHeaders", "").split(",");
        csvSeparator = MapUtils.getString(configMap, "csvSeparator", "\\|");
        hasCsvHeader = MapUtils.getBooleanValue(configMap, "hasCsvHeader", false);
    }

    @Override
    public SourceFunction<Object> getSourceFunction() {
        return this;
    }

    @Override
    public void run(SourceContext<Object> ctx) throws Exception {
        while (this.isRunning) {
            synchronized (ctx.getCheckpointLock()) {
                //获取path目录下的所有文件
                List<String> collectFiles = new ArrayList<>();
                for (String baseDir : path.split(",")) {
                    collectFiles(baseDir, collectFiles, batchCount);
                    if (collectFiles.size() >= batchCount) break;
                }
                //读取剩余的文件
                collectFiles.forEach(filePath -> handleFile(ctx, filePath, null, fileType));
            }
        }
    }

    /**
     * 收集文件
     *
     * @param path 文件路径
     */
    public void collectFiles(String path, List<String> collectFiles, int batchCount) {
        File baseFile = new File(path);
        if (!baseFile.exists()) return;

        Collection<File> files = new ArrayList<>();
        if (baseFile.isDirectory()) {
            files = FileUtils.listFiles(baseFile, null, true);
        } else {
            files.add(baseFile);
        }
        for (File file : files) {
            //过滤正在写入的文件,文件写入时间大于2分钟
            if (System.currentTimeMillis() - file.lastModified() < 1000 * 60 * 2L) continue;

            //过滤数据
            if (filterFile(file.getAbsolutePath())) continue;

            collectFiles.add(file.getAbsolutePath());
            if (batchCount > 0 && collectFiles.size() >= batchCount) return;
        }
    }

    /**
     * 处理文件
     *
     * @param ctx      flink context
     * @param filePath 文件路径
     * @param bytes    文件字节数组
     * @param fileType 文件类型
     */
    public void handleFile(SourceContext<Object> ctx, String filePath, byte[] bytes, String fileType) {
        log.info("readFile:" + filePath + ",fileType:" + fileType);
        try {
            long counter = 0;
            //读取json文件
            if ("avro".equalsIgnoreCase(fileType)) {
                List<Map<String, Object>> maps = AvroUtil.readFile(filePath);
                for (Map<String, Object> data : maps) {
                    ctx.collect(data);
                    sourceCounter.inc(1L);
                }
            }
            //读取parquet文件
            else if ("parquet".equalsIgnoreCase(fileType)) {
                List<Map<String, Object>> maps = ParquetUtil.readFile(filePath);
                for (Map<String, Object> data : maps) {
                    ctx.collect(data);
                    sourceCounter.inc(1L);
                }
            }
            //普通文件类型 txt、csv、json等
            else {
                BufferedReader bufferedReader = null;
                if (bytes != null && bytes.length > 0) {
                    bufferedReader = new BufferedReader(new StringReader(new String(bytes)));
                } else {
                    bufferedReader = new BufferedReader(new FileReader(filePath));
                }
                String line = null;
                while ((line = bufferedReader.readLine()) != null) {
                    Object object = null;
                    //读取json文件
                    if ("json".equalsIgnoreCase(fileType)) {
                        Map data = JSON.parseObject(line, Map.class);
                        data.put("id", MD5Util.md5(data.get("ip") + "_" + data.get("port")));
                        Object taskId = data.get("task_id");
                        if (taskId instanceof JSONArray) {
                            taskId = ((JSONArray) taskId).get(0);
                        }
                        data.put("task_id", taskId);
                        object = data;
                    }
                    //csv格式
                    else if ("csv".equalsIgnoreCase(fileType)) {
                        if (hasCsvHeader && counter++ == 0) continue;
                        String[] fieldValues = line.split(csvSeparator);
                        if (fieldValues.length != csvHeaders.length)
                            throw new IllegalArgumentException("csvHeader not match! [" + StringUtils.join(csvHeaders, ",") + "] [" + StringUtils.join(fieldValues, ",") + "]");
                        Map<String, Object> map = new HashMap<>();
                        for (int i = 0; i < fieldValues.length; i++) {
                            map.put(csvHeaders[i], fieldValues[i]);
                        }
                        object = map;
                    }
                    //默认文件类型txt
                    else {
                        object = line;
                    }
                    ctx.collect(object);
                    sourceCounter.inc(1L);
                }
                IOUtils.closeQuietly(bufferedReader);
            }
            cacheFile(filePath);
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage());
        }
    }

    /**
     * 判断文件是否已经处理过
     *
     * @param filePath 文件路径
     * @return boolean
     */
    public boolean filterFile(String filePath) {
        boolean flag = false;
        switch (filterMode) {
            case "checkpoint":
                flag = handledFileNames.contains(filePath);
                break;
            case "redis":
                JedisCommands jedis = null;
                try {
                    jedis = JedisUtil.getJedis();
                    String exists = jedis.hget(filterRedisPrefix, filePath);
                    if (StringUtils.isNotEmpty(exists)) {
                        return true;
                    }
                } catch (Exception ex) {
                    log.error(ex.getLocalizedMessage());
                } finally {
                    JedisUtil.close(jedis);
                }
                break;
            case "delete":
                break;
        }
        return flag;
    }

    /**
     * 缓存文件信息
     *
     * @param filePath 文件路径
     */
    public void cacheFile(String filePath) {
        switch (filterMode.toLowerCase()) {
            case "checkpoint":
                handledFileNames.add(filePath);
                break;
            case "redis":
                JedisCommands jedis = null;
                try {
                    jedis = JedisUtil.getJedis();
                    jedis.hset(filterRedisPrefix, filePath, String.valueOf(System.currentTimeMillis()));
                } catch (Exception ex) {
                    log.error(ex.getLocalizedMessage());
                } finally {
                    JedisUtil.close(jedis);
                }
                break;
            case "delete":
                FileUtils.deleteQuietly(new File(filePath));
                break;
        }
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }

    @Override
    public void close() throws Exception {
        this.cancel();
        super.close();
    }

    //=============================================  指标  ==============================================================//
    private transient Counter sourceCounter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        sourceCounter = getRuntimeContext().getMetricGroup().counter(this.getClass().getSimpleName() + "-source-counter");
    }

    //=============================================  备份检查点  ==============================================================//
    private volatile boolean isRunning = true;

    //记录处理过的文件名称
    private List<String> handledFileNames = new ArrayList<>();
    private transient ListState<String> handleFileNamesState;

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        this.handleFileNamesState.clear();
        this.handleFileNamesState.addAll(handledFileNames);

        log.info("handledFileCount:" + handledFileNames.size() + ",lineCount:" + sourceCounter.getCount());
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        handleFileNamesState = context.getOperatorStateStore().getListState(
                new ListStateDescriptor<>(
                        this.getClass().getSimpleName() + "-source-handlefilenames",
                        String.class));

        if (context.isRestored()) {
            for (String currentFileName : handleFileNamesState.get()) {
                handledFileNames.add(currentFileName);
            }
            log.info("restored handlefilenames:" + StringUtils.join(handledFileNames, ","));
        } else {
            log.info("No restore state for baseFileSource.");
        }
    }
}
