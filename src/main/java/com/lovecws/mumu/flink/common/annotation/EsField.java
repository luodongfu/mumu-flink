package com.lovecws.mumu.flink.common.annotation;

import java.lang.annotation.*;

/**
 * es存储字段注解
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface EsField {

    /**
     * 字段存储方式 es字段名称
     *
     * @return
     */
    public String name();

    /**
     * 存储数据类型 es字段类型
     *
     * @return
     */
    public String type() default "keyword";

    /**
     * 存储数据类型 日期格式
     *
     * @return
     */
    public String format() default "";

    /**
     * 存储数据类型 es字段分析
     *
     * @return
     */

    public String analyze() default "not_analyzed";
}
