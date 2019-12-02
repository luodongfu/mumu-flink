package com.lovecws.mumu.flink.common.util;

import org.junit.Test;

/**
 * @program: trunk
 * @description: ip工具类测试
 * @author: 甘亮
 * @create: 2019-09-09 15:14
 **/
public class IpUtilTest {

    @Test
    public void ip2value(){
        long l = IPUtil.ipToLong("172.31.132.1");
        System.out.println(l);
    }
}
