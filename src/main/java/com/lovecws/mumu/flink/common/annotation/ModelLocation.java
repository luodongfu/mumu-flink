package com.lovecws.mumu.flink.common.annotation;

import java.lang.annotation.*;

/**
 * 如果接收的数据是csv格式，该注解标注 字段在行的索引位置
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ModelLocation {

    /**
     * 字段所处行的索引位置信息
     *
     * @return
     */
    public int index() default 0;

    /**
     * 字段类型  int,long,float,date
     *
     * @return
     */
    public String type() default "string";

    /**
     * 字段值格式 如果type为date类型 免责format为日期格式yyyy-MM-dd HH:mm:ss
     *
     * @return
     */
    public String format() default "";

    /**
     * 如果字段不存在 则设置为默认值
     *
     * @return
     */
    public String defaultVal() default "";
}
