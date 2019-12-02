package com.lovecws.mumu.flink.streaming.handler;

import com.alibaba.fastjson.JSON;
import com.lovecws.mumu.flink.streaming.common.handler.IndustryHandler;
import org.junit.Test;

import java.util.Map;

/**
 * @program: act-able
 * @description: 行业分类接口测试
 * @author: 甘亮
 * @create: 2019-06-10 17:23
 **/
public class IndustryHandlerTest {

    private IndustryHandler industryHandler=new IndustryHandler();

    @Test
    public void handler() {
        Map<String, Object> handler = industryHandler.handler("深圳前海小鸟云计算有限公司");
        System.out.println(JSON.toJSONString(handler));
    }

    @Test
    public void getIndustry() {
        IndustryHandler industryHandler = new IndustryHandler();
        industryHandler.getIndustry("http://39.98.202.40:8890/apistore/company/companyInfo"
                , "bearer 1c1ce451-6940-4c70-857b-cb35b4f29f9f",
                "深圳前海小鸟云计算有限公司");
    }
}
