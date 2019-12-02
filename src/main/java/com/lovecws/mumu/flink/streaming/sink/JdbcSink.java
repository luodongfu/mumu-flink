package com.lovecws.mumu.flink.streaming.sink;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.jdbc.AbstractJdbcService;
import com.lovecws.mumu.flink.common.jdbc.BasicJdbcService;
import com.lovecws.mumu.flink.common.jdbc.JdbcConfig;
import com.lovecws.mumu.flink.common.util.MapFieldUtil;
import com.lovecws.mumu.flink.common.util.StorageFilterUtil;
import com.lovecws.mumu.flink.common.util.TableUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @program: trunk
 * @description: jdbc存储
 * @author: 甘亮
 * @create: 2019-11-05 14:57
 **/
@Slf4j
@ModularAnnotation(type = "sink", name = "jdbc")
public class JdbcSink extends RichSinkFunction<Object> implements BaseSink {

    public JdbcConfig jdbcConfig;
    public AbstractJdbcService jdbcService;
    public boolean init = false;
    public String table;


    //存储数据的字段
    public Map<String, Object> tableInfo;
    public List<Map<String, String>> columns;
    //存储数据的存储方式
    public String storage;

    //排除数据
    private String filter;
    //排除字段
    private String[] excludeFields;

    public String url;
    public String driver;
    public String user;
    public String password;
    public String charset;
    private String databaseType;

    public JdbcSink(Map<String, Object> configMap) {
        if (configMap == null) {
            configMap = new HashMap<>();
        }
        table = MapUtils.getString(configMap, "table", "");
        filter = MapUtils.getString(configMap, "filter", "");
        excludeFields = MapUtils.getString(configMap, "excludeFields", "").split(",");

        url = MapUtils.getString(configMap, "url", MapFieldUtil.getMapField(configMap, "spring.datasource.default.url").toString());
        driver = MapUtils.getString(configMap, "driver", MapFieldUtil.getMapField(configMap, "spring.datasource.default.driver-class-name").toString());
        user = MapUtils.getString(configMap, "user", MapFieldUtil.getMapField(configMap, "spring.datasource.default.username").toString());
        password = MapUtils.getString(configMap, "password", MapFieldUtil.getMapField(configMap, "spring.datasource.default.password").toString());

        if (StringUtils.isEmpty(url) || StringUtils.isEmpty(table))
            throw new IllegalArgumentException("illegal jdbc url:" + url + " or table is empty");

        //判断数据存储类型 hive、atdpurge、jdbc
        databaseType = MapUtils.getString(configMap, "type", "");
    }

    @Override
    public void invoke(Object object, Context context) throws Exception {
        if (object == null) return;
        //上层传递的数据有可能是集合
        List<Object> datas = new ArrayList<>();
        if (object instanceof List) {
            datas.addAll((List) object);
        } else {
            datas.add(object);
        }
        //初始化
        init(datas.get(0));

        List<Map<String, Object>> _datas = new ArrayList<>();
        for (Object data : datas) {
            Map<String, Object> cloumnValueMap = TableUtil.getCloumnValues(data, columns);
            //是否过滤该条数据，满足则直接返回
            if (StorageFilterUtil.filter(cloumnValueMap, filter)) {
                continue;
            }
            for (String excludeField : excludeFields) {
                if (cloumnValueMap.containsKey(excludeField)) cloumnValueMap.remove(excludeField);
            }
            _datas.add(cloumnValueMap);
        }
        handleData(_datas);
    }

    public void handleData(List<Map<String, Object>> datas) {
        if (datas.size() > 1)
            log.info("jdbcSinkInvoke:{table:" + table + ",size:" + datas.size() + "}");
        jdbcService.batchInsertInto(table, columns, datas);
    }

    /**
     * 初始化 jdbc存储，检查表是否存在，如果不存在 则根据对象进行表创建。
     *
     * @param object 对象
     */
    public void init(Object object) {
        try {
            if (!init) {
                tableInfo = TableUtil.getTableInfo(object);
                columns = (List<Map<String, String>>) tableInfo.get("columns");
                storage = MapUtils.getMap(tableInfo, "properties", new HashMap()).getOrDefault("storage", "").toString();
                if (!jdbcService.tableExists(table)) {
                    boolean success = jdbcService.createTable(jdbcService.getCreateTableSql(table, tableInfo));
                    log.debug("create table " + (success ? "success" : "fail"));
                }
            }
            init = true;
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage());
        }
    }

    @Override
    public SinkFunction<Object> getSinkFunction() {
        return this;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if ("jdbc".equalsIgnoreCase(databaseType)) {
            jdbcConfig = new JdbcConfig(url, driver, user, password);
            jdbcService = new BasicJdbcService(jdbcConfig);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
