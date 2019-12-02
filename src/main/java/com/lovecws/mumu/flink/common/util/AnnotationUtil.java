package com.lovecws.mumu.flink.common.util;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import org.apache.log4j.Logger;
import org.reflections.Reflections;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 注解工具类
 */
public class AnnotationUtil implements Serializable {

    private static final Logger log = Logger.getLogger(AnnotationUtil.class);


    private static final ConcurrentHashMap<String, Map<String, Class>> MODULAR_ANNOTATION_CLASS = new ConcurrentHashMap<String, Map<String, Class>>();

    /**
     * @param packageName 初始化包路径
     * @param type        模块类型
     * @param name        模块名称
     *                    TODO 反射添加缓存 将第一次反射出来的结果缓存到map中 下次直接从缓存获取
     */
    public static Class getModularAnnotation(String packageName, String type, String name) {
        try {
            Map<String, Class> packageMap = MODULAR_ANNOTATION_CLASS.get(packageName);
            if (packageMap == null) {
                Reflections f = new Reflections(packageName);
                Set<Class<?>> set = f.getTypesAnnotatedWith(ModularAnnotation.class);
                packageMap = new HashMap<>();
                for (Class<?> c : set) {
                    ModularAnnotation annotation = c.getAnnotation(ModularAnnotation.class);
                    if (annotation == null) continue;
                    packageMap.put(annotation.type() + ":" + annotation.name(), c);
                }
                MODULAR_ANNOTATION_CLASS.put(packageName, packageMap);
            }

            //便利packageMap
            for (Map.Entry<String, Class> entry : packageMap.entrySet()) {
                String typeName = entry.getKey();
                String[] typeNames = typeName.split(":");
                String annotationType = typeNames[0];
                String annotationName = typeNames[1];
                if (annotationType.equalsIgnoreCase(type) && (annotationName.equalsIgnoreCase(name) || name.toLowerCase().startsWith(annotationName.toLowerCase()))) {
                    return entry.getValue();
                }
            }
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
        return null;
    }

    /**
     * @param type 初始化包路径
     * @param name 初始化包路径
     */
    public static Class getModularAnnotation(String type, String name) {
        String packageName = "com.lovecws.mumu.flink";
        return getModularAnnotation(packageName, type, name);
    }
}
