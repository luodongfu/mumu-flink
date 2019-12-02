package com.lovecws.mumu.flink.common.util;

import org.junit.Test;

/**
 * @program: trunk
 * @description: HttpClientUtil测试
 * @author: 甘亮
 * @create: 2019-09-27 18:46
 **/
public class HttpClientUtilTest {

    @Test
    public void get(){
        String content = HttpClientUtil.get("https://www.imooc.com/wenda123/detail/3954701");
        System.out.println(content);
    }
}
