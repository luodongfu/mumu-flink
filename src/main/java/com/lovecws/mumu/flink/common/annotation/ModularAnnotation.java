package com.lovecws.mumu.flink.common.annotation;

import java.lang.annotation.*;

/**
 * handler处理流程注解
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ModularAnnotation {

    /**
     * 模块类型 storage、backup、handler
     *
     * @return
     */
    public String type();

    /**
     * 模块名称
     *
     * @return
     */
    public String name();

}
