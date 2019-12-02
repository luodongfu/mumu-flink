package com.lovecws.mumu.flink.streaming.config;

import com.lovecws.mumu.flink.common.config.ConfigProperties;
import com.lovecws.mumu.flink.common.util.AnnotationUtil;
import com.lovecws.mumu.flink.streaming.backup.BaseBackup;
import com.lovecws.mumu.flink.streaming.duplicator.BaseDuplicator;
import com.lovecws.mumu.flink.streaming.sink.BaseSink;
import com.lovecws.mumu.flink.streaming.source.BaseSource;
import com.lovecws.mumu.flink.streaming.task.AbstractStreamingTask;
import com.lovecws.mumu.flink.streaming.validation.BaseValidation;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.ConstructorUtils;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 配置管理器
 */
@Slf4j
public class ConfigManager implements Serializable {

    private static final Map<String, Object> dataMap = new ConcurrentHashMap<>();

    /**
     * @param type   事件类型 storage source handler dumplicator
     * @param event  数据源 mapper.atd、jmr、gynetres
     * @param active hive,atdpurge
     * @return Object
     */
    private Object getObject(String type, String event, String active) {
        event = event.toLowerCase();
        String key = type + "_" + event + "_" + active;
        Object dataObject = dataMap.get(key);
        String currencyEvent = getEventKey(event);
        try {
            if (dataObject == null) {
                Map<String, Object> configMap = ConfigProperties.getMap("streaming.tasks." + currencyEvent + "." + type + "." + active);
                configMap.put("defaults", ConfigProperties.getMap("streaming.defaults"));
                configMap.put("spring", ConfigProperties.getMap("spring"));
                configMap.put("configManager", this);
                configMap.put("event", currencyEvent);

                Class objectClass = AnnotationUtil.getModularAnnotation(type, active);
                if (objectClass == null) {
                    throw new IllegalArgumentException("unsupport data " + currencyEvent + " " + type + " [" + active + "]");
                }
                Constructor constructor = objectClass.getConstructor(Map.class);
                dataObject = constructor.newInstance(configMap);

                dataMap.put(key, dataObject);
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        }
        return dataObject;
    }

    public AbstractStreamingTask getStreamingTask(String event) {
        try {
            Class<AbstractStreamingTask> eventClass = AnnotationUtil.getModularAnnotation("task", event);
            Map<String, Object> configMap = ConfigProperties.getMap("streaming.tasks." + event + ".config");
            configMap.put("event", event);
            configMap.put("configManager", this);
            configMap.put("defaults", ConfigProperties.getMap("streaming.defaults"));
            configMap.put("spring", ConfigProperties.getMap("spring"));
            return ConstructorUtils.invokeConstructor(eventClass, configMap);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new IllegalArgumentException(ex);
        }
    }

    /**
     * 数据来源 不支持多数据源
     *
     * @param event 数据源 mapper.atd、jmr、gynetres、dpi
     * @return BaseSource
     */
    public BaseSource getSource(String event) {
        String property = ConfigProperties.getString("streaming.tasks." + getEventKey(event) + ".source.active");
        return (BaseSource) getObject("source", event, property);
    }

    /**
     * 获取数据存储策略，支持多中存储方式
     *
     * @param event 数据源 mapper.atd
     * @return List<BaseSink>
     */
    public List<BaseSink> getSinks(String event) {
        String property = ConfigProperties.getString("streaming.tasks." + getEventKey(event) + ".sink.active");
        if (property == null) throw new IllegalArgumentException();

        List<BaseSink> sinks = new ArrayList<>();
        String[] storageTypes = property.split(",");
        if (storageTypes.length > 1) {
            for (String storageType : storageTypes) {
                sinks.add((BaseSink) getObject("sink", event, storageType));
            }
        } else {
            sinks.add((BaseSink) getObject("sink", event, property));
        }
        return sinks;
    }

    /**
     * 获取原始文件备份sink
     *
     * @param event 事件
     * @return List<BaseBackup>
     */
    public List<BaseBackup> getBackups(String event) {
        String property = ConfigProperties.getString("streaming.tasks." + getEventKey(event) + ".backup.active");
        if (property == null || "".equals(property)) return null;

        List<BaseBackup> sinks = new ArrayList<>();
        String[] backupTypes = property.split(",");
        if (backupTypes.length > 1) {
            for (String backupType : backupTypes) {
                sinks.add((BaseBackup) getObject("backup", event, backupType));
            }
        } else {
            sinks.add((BaseBackup) getObject("backup", event, property));
        }
        return sinks;
    }

    /**
     * 获取数据备份
     *
     * @param event 事件
     * @return BaseDuplicator
     */
    public BaseDuplicator getDuplicator(String event) {
        String property = ConfigProperties.getString("streaming.tasks." + getEventKey(event) + ".duplicator.active");
        if (property == null || "".equals(property)) return null;
        return (BaseDuplicator) getObject("duplicator", event, property);
    }

    /**
     * 获取数据校验 对数据进行类型 值校验，过滤不符合规则的数据
     *
     * @param event 事件
     * @return BaseValidation
     */
    public BaseValidation getValidation(String event) {
        String property = ConfigProperties.getString("streaming.tasks." + getEventKey(event) + ".validation.active");
        if (property == null || "".equals(property)) return null;
        return (BaseValidation) getObject("validation", event, property);
    }

    /**
     * 获取数据校验 对数据进行类型 值校验，过滤不符合规则的数据
     *
     * @param handler 事件
     * @return BaseValidation
     */
    public <T> T getBean(String type, String handler) {
        try {
            String key = type + "_" + handler;
            Object dataObject = dataMap.get(key);
            if (dataObject == null) {
                Class<T> modularAnnotation = AnnotationUtil.getModularAnnotation(type, handler);
                if (modularAnnotation == null) throw new IllegalArgumentException();
                dataObject = ConstructorUtils.invokeConstructor(modularAnnotation);
                dataMap.put(key, dataObject);
            }
            return (T) dataObject;
        } catch (Exception e) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * 获取实体
     *
     * @return T
     */
    public <T> T getBean(Class<T> tClass) {
        try {
            String key = tClass.getCanonicalName();
            Object dataObject = dataMap.get(key);
            if (dataObject == null) {
                dataObject = ConstructorUtils.invokeConstructor(tClass);
                dataMap.put(key, dataObject);
            }
            return (T) dataObject;
        } catch (Exception e) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * 获取数据校验 对数据进行类型 值校验，过滤不符合规则的数据
     *
     * @param handler 事件
     * @return T
     */
    public <T> T getHandler(String handler) {
        return getBean("handler", handler);
    }

    /**
     * 获取数据校验 对数据进行类型 值校验，过滤不符合规则的数据
     *
     * @param handler 事件
     * @return BaseValidation
     */
    public <T> T getCache(String handler) {
        return getBean("cache", handler);
    }

    /**
     * 获取任务的event ,当任务设置为currency执行的时候，event会默认加上 event-1标识符，所以在这里还原真实任务信息
     *
     * @param event 并发任务key mapper.atd-1
     * @return String
     */
    private String getEventKey(String event) {
        String taskEvent = event.toLowerCase();
        int indexOf = taskEvent.lastIndexOf("-");
        if (indexOf > -1) taskEvent = taskEvent.substring(0, indexOf);
        return taskEvent;
    }
}
