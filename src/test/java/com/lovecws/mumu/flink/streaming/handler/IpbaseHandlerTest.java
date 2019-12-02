package com.lovecws.mumu.flink.streaming.handler;

import com.alibaba.fastjson.JSON;
import com.lovecws.mumu.flink.streaming.common.handler.IpbaseHandler;
import org.junit.Test;

import java.util.Map;

/**
 * @program: act-able
 * @description: 基础资源接口查询
 * @author: 甘亮
 * @create: 2019-06-10 17:23
 **/
public class IpbaseHandlerTest {

    private IpbaseHandler ipbaseHandler=new IpbaseHandler();

    @Test
    public void handler() {
        Map<String, Object> handler = ipbaseHandler.handler("110.53.72.27");
        System.out.println(JSON.toJSONString(handler));
    }

}
