package com.lovecws.mumu.flink.common.util;

import org.junit.Test;

/**
 * @program: mumu-flink
 * @description: EscapeUtil测试
 * @author: 甘亮
 * @create: 2019-06-20 11:33
 **/
public class EscapeUtilTest {

    @Test
    public void escape(){
        String escape = EscapeUtil.escape("lovecws\n\t''");
        System.out.println(escape);
    }
}
