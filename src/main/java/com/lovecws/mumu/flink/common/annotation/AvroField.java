package com.lovecws.mumu.flink.common.annotation;

import java.lang.annotation.*;

/**
 * @program: act-able
 * @description: 如果接收的数据为avro格式 该注解标注avro的schema
 * @author: 甘亮
 * @create: 2019-06-06 10:13
 **/
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface AvroField {

    /**
     * avro字段名称
     *
     * @return
     */
    public String name();

    /**
     * avro字段类型
     *
     * @return
     */
    public String[] type() default {"string", "null"};
}
