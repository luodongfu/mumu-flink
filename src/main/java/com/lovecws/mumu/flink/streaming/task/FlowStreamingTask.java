package com.lovecws.mumu.flink.streaming.task;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.model.attack.FlowEventModel;
import com.lovecws.mumu.flink.common.util.DateUtils;
import com.lovecws.mumu.flink.common.util.IPUtil;
import com.lovecws.mumu.flink.common.util.MD5Util;
import com.lovecws.mumu.flink.common.util.MapFieldUtil;
import com.lovecws.mumu.flink.streaming.common.handler.IndustryHandler;
import com.lovecws.mumu.flink.streaming.common.handler.IpipnetHandler;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @program: act-able
 * @description: 第三方流量统计数据
 * @author: 甘亮
 * @create: 2019-07-18 09:58
 **/
@Slf4j
@ModularAnnotation(type = "task", name = "flow")
public class FlowStreamingTask extends AbstractStreamingTask {

    private IpipnetHandler ipipnetHandler;
    private IndustryHandler industryHandler;

    public FlowStreamingTask(Map<String, Object> configMap) {
        super(configMap);
        ipipnetHandler = new IpipnetHandler();
        industryHandler = new IndustryHandler();
    }

    @Override
    public Object parseData(Object data) {
        List<FlowEventModel> flowEventModels = new ArrayList<>();
        if (data == null) return flowEventModels;
        String line = new String((byte[]) data, StandardCharsets.UTF_8);
        try {
            String dataJson = StringEscapeUtils.unescapeJson(line);
            if (dataJson.startsWith("\"")) dataJson = dataJson.substring(1);
            if (dataJson.endsWith("\"")) dataJson = dataJson.substring(0, dataJson.length() - 1);
            Map flowMap = JSON.parseObject(dataJson, Map.class);
            List<Map<String, Object>> ListFlows = (List<Map<String, Object>>) flowMap.getOrDefault("ListFlowCount", new ArrayList<>());
            if (ListFlows == null || ListFlows.size() == 0) return null;
            flowMap.remove("ListFlowCount");

            List<FlowEventModel> finalFlowEventModels = flowEventModels;
            ListFlows.forEach(listFlowMap -> {
                listFlowMap.putAll(flowMap);
                finalFlowEventModels.add(JSON.parseObject(JSON.toJSONString(listFlowMap), FlowEventModel.class));
            });
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage() + "[" + line + "]", ex);
        }
        return flowEventModels;
    }

    @Override
    public Object fillData(Object data) {
        List<FlowEventModel> flowEventModels = (List<FlowEventModel>) data;

        flowEventModels.forEach(flowEventModel -> {
            //源ip
            String sourceIP = flowEventModel.getSourceIP();
            if (StringUtils.isNotEmpty(sourceIP)) {
                flowEventModel.setSourceIpValue(IPUtil.ipToLong(sourceIP));
                flowEventModel.setSourceIpType(IPUtil.ipType(sourceIP));
                //源ip地市信息
                Map<String, Object> srcIpAreaInfo = ipipnetHandler.handler(sourceIP);
                flowEventModel.setSourceIpCountry(srcIpAreaInfo.getOrDefault("contry", "").toString());
                flowEventModel.setSourceIpCountryCode(srcIpAreaInfo.getOrDefault("contryCode", "").toString());
                flowEventModel.setSourceIpProvince(srcIpAreaInfo.getOrDefault("province", "").toString());
                flowEventModel.setSourceIpProvinceCode(srcIpAreaInfo.getOrDefault("provinceCode", "").toString());
                flowEventModel.setSourceIpCity(srcIpAreaInfo.getOrDefault("city", "").toString());
                flowEventModel.setSourceIpCityCode(srcIpAreaInfo.getOrDefault("cityCode", "").toString());
                flowEventModel.setSourceIpOperator(srcIpAreaInfo.getOrDefault("operator", "").toString());
                flowEventModel.setSourceIpLongitude(srcIpAreaInfo.getOrDefault("longitude", "").toString());
                flowEventModel.setSourceIpLatitude(srcIpAreaInfo.getOrDefault("latitude", "").toString());
                flowEventModel.setSourceIpIdc(srcIpAreaInfo.getOrDefault("idc", "").toString());

                //源ip企业信息
                Map<String, Object> industryMap = industryHandler.handler(flowEventModel.getSourceCompany());
                flowEventModel.setSourceIndustryType(MapFieldUtil.getMapFieldValue(industryMap, "category_name", ""));
            }
            //目的ip
            String destIP = flowEventModel.getDestIP();
            if (StringUtils.isNotEmpty(destIP)) {
                flowEventModel.setDestIpValue(IPUtil.ipToLong(destIP));
                flowEventModel.setDestIpType(IPUtil.ipType(destIP));

                //目的ip地市信息
                Map<String, Object> dstIpAreaInfo = ipipnetHandler.handler(flowEventModel.getDestIP());
                flowEventModel.setDestIpCountry(dstIpAreaInfo.getOrDefault("contry", "").toString());
                flowEventModel.setDestIpCountryCode(dstIpAreaInfo.getOrDefault("contryCode", "").toString());
                flowEventModel.setDestIpProvince(dstIpAreaInfo.getOrDefault("province", "").toString());
                flowEventModel.setDestIpProvinceCode(dstIpAreaInfo.getOrDefault("provinceCode", "").toString());
                flowEventModel.setDestIpCity(dstIpAreaInfo.getOrDefault("city", "").toString());
                flowEventModel.setDestIpCityCode(dstIpAreaInfo.getOrDefault("cityCode", "").toString());
                flowEventModel.setDestIpOperator(dstIpAreaInfo.getOrDefault("operator", "").toString());
                flowEventModel.setDestIpLongitude(dstIpAreaInfo.getOrDefault("longitude", "").toString());
                flowEventModel.setDestIpLatitude(dstIpAreaInfo.getOrDefault("latitude", "").toString());
                flowEventModel.setDestIpIdc(dstIpAreaInfo.getOrDefault("idc", "").toString());

                //目的ip企业信息
                Map<String, Object> destCorpInfo = industryHandler.handler(flowEventModel.getDestCompany());
                flowEventModel.setDestIndustryType(MapFieldUtil.getMapFieldValue(destCorpInfo, "category_name", ""));
            }

            flowEventModel.setId(MD5Util.md5(data.toString()));
            flowEventModel.setCreateTime(new Date());
            flowEventModel.setDs(DateUtils.formatDate(flowEventModel.getCreateTime(), "yyyy-MM-dd"));
        });

        return flowEventModels;
    }

    @Override
    public Boolean filterData(Object data) {
        List<FlowEventModel> flowEventModels = (List<FlowEventModel>) data;
        return flowEventModels.size() > 0;
    }
}
