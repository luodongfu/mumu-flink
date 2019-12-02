package com.lovecws.mumu.flink.streaming.task;

import com.lovecws.mumu.flink.common.util.DateUtils;
import com.lovecws.mumu.flink.common.util.MapFieldUtil;
import com.lovecws.mumu.flink.streaming.backup.BaseBackup;
import com.lovecws.mumu.flink.streaming.config.ConfigManager;
import com.lovecws.mumu.flink.streaming.duplicator.BaseDuplicator;
import com.lovecws.mumu.flink.streaming.sink.BaseSink;
import com.lovecws.mumu.flink.streaming.source.BaseSource;
import com.lovecws.mumu.flink.streaming.validation.BaseValidation;
import com.google.common.collect.Lists;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @program: trunk
 * @description: 实时流基类
 * @author: 甘亮
 * @create: 2019-11-01 13:13
 **/
public abstract class AbstractStreamingTask implements Serializable {

    private static final Logger log = Logger.getLogger(AbstractStreamingTask.class);
    public ConfigManager configManager;
    private String event;

    //备份 fs、memory
    private String stateBackend;
    //文件备份路径
    private String checkpointDataUri;
    //备份异步
    private boolean asynchronousSnapshots;
    //备份时间间隔 milliseconds
    private long checkpointInterval;

    //flink流处理并发度
    private int parallelism;//流并发度
    private int maxParallelism;//流最大并发度

    private int windowSize;//时间窗口
    private int slideSize;//滑动窗口 每隔多久生成一个windowSize时间段的窗口

    private int restartAttempts;
    private long delayBetweenAttempts;

    private String dataFlowType;//数据流动类型 streaming batch

    public AbstractStreamingTask(Map<String, Object> configMap) {
        this.event = MapUtils.getString(configMap, "event", "");
        this.configManager = (ConfigManager) configMap.get("configManager");
        if (configManager == null) configManager = new ConfigManager();

        this.stateBackend = MapUtils.getString(configMap, "stateBackend", MapFieldUtil.getMapField(configMap, "defaults.config.stateBackend", "fs").toString());
        this.checkpointDataUri = MapUtils.getString(configMap, "checkpointDataUri", MapFieldUtil.getMapField(configMap, "defaults.config.checkpointDataUri", "/tmp/flink-checkpoints").toString());
        this.checkpointInterval = DateUtils.getExpireTime(MapUtils.getString(configMap, "checkpointInterval", MapFieldUtil.getMapField(configMap, "defaults.config.checkpointInterval", "1m").toString())) * 1000L;
        this.asynchronousSnapshots = MapUtils.getBooleanValue(configMap, "asynchronousSnapshots", Boolean.parseBoolean(MapFieldUtil.getMapField(configMap, "defaults.config.asynchronousSnapshots", "true").toString()));
        if (this.checkpointDataUri.startsWith("/")) this.checkpointDataUri = "file://" + this.checkpointDataUri;

        this.parallelism = MapUtils.getIntValue(configMap, "parallelism", Integer.parseInt(MapFieldUtil.getMapField(configMap, "defaults.config.parallelism", "1").toString()));
        this.maxParallelism = MapUtils.getIntValue(configMap, "maxParallelism", Integer.parseInt(MapFieldUtil.getMapField(configMap, "defaults.config.maxParallelism", "1").toString()));
        if (parallelism > maxParallelism) maxParallelism = parallelism;

        this.windowSize = DateUtils.getExpireTime(MapUtils.getString(configMap, "windowSize", MapFieldUtil.getMapField(configMap, "defaults.config.windowSize", "10m").toString()));
        this.slideSize = DateUtils.getExpireTime(MapUtils.getString(configMap, "slideSize", MapFieldUtil.getMapField(configMap, "defaults.config.slideSize", "1m").toString()));

        this.restartAttempts = Integer.parseInt(MapUtils.getString(configMap, "restartAttempts", MapFieldUtil.getMapField(configMap, "defaults.config.restartAttempts", "10").toString()));
        this.delayBetweenAttempts = DateUtils.getExpireTime(MapUtils.getString(configMap, "delayBetweenAttempts", MapFieldUtil.getMapField(configMap, "defaults.config.delayBetweenAttempts", "10").toString())) * 1000L;

        dataFlowType = MapUtils.getString(configMap, "dataFlowType", "batch");
    }

    public void streaming() {
        try {
            //数据源
            BaseSource dataSource = configManager.getSource(event);
            //数据存储
            List<BaseSink> sinkFunctions = configManager.getSinks(event);
            //数据备份
            List<BaseBackup> backupFunctions = configManager.getBackups(event);
            //数据去重
            BaseDuplicator duplicatorFunction = configManager.getDuplicator(event);
            //数据校验
            BaseValidation validationFunction = configManager.getValidation(event);

            StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

            streamExecutionEnvironment.setParallelism(parallelism);
            streamExecutionEnvironment.setMaxParallelism(maxParallelism);

            streamExecutionEnvironment.getConfig().setExecutionMode(ExecutionMode.PIPELINED);
            //设置状态检查点
            StateBackend backend = null;
            if (stateBackend != null && !"".equals(stateBackend)) {
                if ("fs".equalsIgnoreCase(stateBackend)) {
                    backend = new FsStateBackend(checkpointDataUri, asynchronousSnapshots);
                } else if ("memory".equalsIgnoreCase(stateBackend)) {
                    backend = new MemoryStateBackend();
                } else {
                    stateBackend = null;
                }
                if (stateBackend != null) streamExecutionEnvironment.setStateBackend(backend);
            }
            //设置重启策略
            streamExecutionEnvironment.setRestartStrategy(RestartStrategies.fixedDelayRestart(restartAttempts, delayBetweenAttempts));
            //当设置检查点时间大于0的时候开启定时检查点备份
            if (checkpointInterval > 0) {
                streamExecutionEnvironment.enableCheckpointing(checkpointInterval);
                // 设置模式为exactly-once （这是默认值）
                streamExecutionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
                // 确保检查点之间有进行500 ms的进度
                streamExecutionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
                // 检查点必须在一分钟内完成，或者被丢弃
                streamExecutionEnvironment.getCheckpointConfig().setCheckpointTimeout(600000);
                // 同一时间只允许进行一个检查点
                streamExecutionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
                streamExecutionEnvironment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            }

            //设置全局配置信息
            Configuration configuration = new Configuration();
            streamExecutionEnvironment.getConfig().setGlobalJobParameters(configuration);

            //获取数据源
            DataStream<Object> streamSource = streamExecutionEnvironment.addSource(dataSource.getSourceFunction()).name("streamSource");

            //数据备份
            if (backupFunctions != null)
                backupFunctions.forEach(backupFunction -> streamSource.addSink(backupFunction.getBackupFunction()));

            //数据解析
            SingleOutputStreamOperator<Object> stream = streamSource.map(new MapFunction<Object, Object>() {
                @Override
                public Object map(Object value) throws Exception {
                    return parseData(value);
                }
            }).name("streamParse");

            //数据校验
            if (validationFunction != null)
                stream = stream.filter(validationFunction.getValidationFunction()).name("streamValidation");

            //数据填充
            stream = stream.map(new MapFunction<Object, Object>() {
                @Override
                public Object map(Object value) throws Exception {
                    return fillData(value);
                }
            }).name("streamFillout");
            //数据过滤
            stream = stream.filter(new FilterFunction<Object>() {
                @Override
                public boolean filter(Object value) throws Exception {
                    return filterData(value);
                }
            }).name("streamFilter");

            //数据重复检测
            if (duplicatorFunction != null)
                stream = stream.filter(duplicatorFunction.getDuplicatorFunction()).name("streamDuplicator");

            SingleOutputStreamOperator<Object> sinkStream = null;
            if ("streaming".equalsIgnoreCase(dataFlowType)) {
                sinkStream = stream;
            } else if ("batch".equalsIgnoreCase(dataFlowType)) {
                //按照时间窗口 数据批量化
                sinkStream = stream.timeWindowAll(Time.seconds(10)).apply(new AllWindowFunction<Object, Object, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Object> values, Collector<Object> out) throws Exception {
                        out.collect(Lists.newArrayList(values));
                    }
                }).name("streamtimeWindowAllBatch");
            }


            //数据存储到hive、es、本地...等多個通道
            final SingleOutputStreamOperator<Object> dataSinkStream = sinkStream;
            if (sinkFunctions != null)
                sinkFunctions.forEach(sinkFunction -> dataSinkStream.addSink(sinkFunction.getSinkFunction()).name(sinkFunction.getClass().getSimpleName()));

            //时间窗口 统计时间段的数据量
            /*stream.timeWindowAll(Time.seconds(windowSize), Time.seconds(slideSize)).process(new ProcessAllWindowFunction<Object, Object, TimeWindow>() {
                @Override
                public void process(Context context, Iterable<Object> elements, Collector<Object> out) throws Exception {
                    List list = IteratorUtils.toList(elements.iterator());
                    log.info(" event [" + event + "] every [" + slideSize + "s] show latest [" + windowSize + "s] data[" + list.size() + "]");
                }
            }).name("streamKeyWindow");*/
            stream.map(new MapFunction<Object, Tuple2<String, Long>>() {
                @Override
                public Tuple2<String, Long> map(Object o) throws Exception {
                    return new Tuple2<>("1", 1L);
                }
            }).name("MapFunction")
                    .keyBy(0).timeWindow(Time.seconds(windowSize), Time.seconds(slideSize))
                    .sum(1).name("windowCount")
                    .process(new ProcessFunction<Tuple2<String, Long>, Object>() {
                        @Override
                        public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Object> out) throws Exception {
                            log.info(" event [" + event + "] every [" + slideSize + "s] show latest [" + windowSize + "s] data[" + value.f1 + "]");
                        }
                    }).name("streamKeyWindow");
            //将检查点保存到checkpoint中

            streamExecutionEnvironment.execute("streaming-" + event + "-" + DateUtils.formatDate(new Date(), "yyyyMMddHHmmss"));
        } catch (Exception ex) {
            log.error(ex);
            ex.printStackTrace();
        }
    }


    /**
     * 解析数据
     *
     * @param data 解析的一行数据
     * @return
     */
    public abstract Object parseData(Object data);

    /**
     * 数据过滤 返回true的数据保留，返回false的数据被丢弃掉
     *
     * @param data 过滤该行数据
     * @return Boolean
     */
    public abstract Boolean filterData(Object data);


    /**
     * 填充数据
     *
     * @param data 填充数据
     * @return
     */
    public abstract Object fillData(Object data);
}
