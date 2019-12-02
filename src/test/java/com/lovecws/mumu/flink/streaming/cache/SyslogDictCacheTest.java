package com.lovecws.mumu.flink.streaming.cache;


import com.lovecws.mumu.flink.streaming.common.cache.SyslogDictCache;
import org.junit.Test;


/**
 * @program: trunk
 * @description: SyslogDictCache缓存测试
 * @author: 甘亮
 * @create: 2019-10-24 14:35
 **/
public class SyslogDictCacheTest {


    private SyslogDictCache syslogDictCache = new SyslogDictCache();

    @Test
    public void getSyslogSeverity() {
        System.out.println(syslogDictCache.getSyslogSeverity("1"));
    }
}
