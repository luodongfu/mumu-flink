package com.lovecws.mumu.flink.streaming.task;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.model.attack.AttackEventModel;
import com.lovecws.mumu.flink.common.util.DateUtils;
import com.lovecws.mumu.flink.common.util.IPUtil;
import com.lovecws.mumu.flink.common.util.MD5Util;
import com.lovecws.mumu.flink.common.util.MapFieldUtil;
import com.lovecws.mumu.flink.streaming.common.handler.IndustryHandler;
import com.lovecws.mumu.flink.streaming.common.handler.IpipnetHandler;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;

/**
 * @program: act-able
 * @description: 第三方預警攻擊事件
 * @author: 甘亮
 * @create: 2019-06-13 09:58
 **/
@Slf4j
@ModularAnnotation(type = "task", name = "attack")
public class AttackStreamingTask extends AbstractStreamingTask {

    private IpipnetHandler ipipnetHandler;
    private IndustryHandler industryHandler;

    public AttackStreamingTask(Map<String, Object> configMap) {
        super(configMap);
        ipipnetHandler = new IpipnetHandler();
        industryHandler = new IndustryHandler();
    }

    @Override
    public Object parseData(Object data) {

        AttackEventModel attackEventModel = new AttackEventModel();
        if (data == null) return attackEventModel;
        String line = new String((byte[]) data, StandardCharsets.UTF_8);
        try {
            attackEventModel = JSON.parseObject(line, AttackEventModel.class);

            attackEventModel.setId(MD5Util.md5(line));
            attackEventModel.setCreateTime(new Date());
            attackEventModel.setDs(DateUtils.formatDate(attackEventModel.getCreateTime(), "yyyy-MM-dd"));
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage() + "[" + line + "]", ex);
        }
        return attackEventModel;
    }

    @Override
    public Object fillData(Object data) {
        AttackEventModel attackEventModel = (AttackEventModel) data;
        //源ip
        String sourceIP = attackEventModel.getSourceIP();
        if (StringUtils.isNotEmpty(sourceIP)) {
            attackEventModel.setSourceIpValue(IPUtil.ipToLong(sourceIP));
            attackEventModel.setSourceIpType(IPUtil.ipType(sourceIP));
            //源ip地市信息
            Map<String, Object> srcIpAreaInfo = ipipnetHandler.handler(sourceIP);
            attackEventModel.setSourceIpCountry(srcIpAreaInfo.getOrDefault("contry", "").toString());
            attackEventModel.setSourceIpCountryCode(srcIpAreaInfo.getOrDefault("contryCode", "").toString());
            attackEventModel.setSourceIpProvince(srcIpAreaInfo.getOrDefault("province", "").toString());
            attackEventModel.setSourceIpProvinceCode(srcIpAreaInfo.getOrDefault("provinceCode", "").toString());
            attackEventModel.setSourceIpCity(srcIpAreaInfo.getOrDefault("city", "").toString());
            attackEventModel.setSourceIpCityCode(srcIpAreaInfo.getOrDefault("cityCode", "").toString());
            attackEventModel.setSourceIpOperator(srcIpAreaInfo.getOrDefault("operator", "").toString());
            attackEventModel.setSourceIpLongitude(srcIpAreaInfo.getOrDefault("longitude", "").toString());
            attackEventModel.setSourceIpLatitude(srcIpAreaInfo.getOrDefault("latitude", "").toString());
            attackEventModel.setSourceIpIdc(srcIpAreaInfo.getOrDefault("idc", "").toString());

            //源ip企业信息
            Map<String, Object> industryMap = industryHandler.handler(attackEventModel.getSourceCompany());
            attackEventModel.setSourceIndustryType(MapFieldUtil.getMapFieldValue(industryMap, "category_name", ""));
        }
        //目的ip
        String destIP = attackEventModel.getDestIP();
        if (StringUtils.isNotEmpty(destIP)) {
            attackEventModel.setDestIpValue(IPUtil.ipToLong(destIP));
            attackEventModel.setDestIpType(IPUtil.ipType(destIP));

            //目的ip地市信息
            Map<String, Object> dstIpAreaInfo = ipipnetHandler.handler(attackEventModel.getDestIP());
            attackEventModel.setDestIpCountry(dstIpAreaInfo.getOrDefault("contry", "").toString());
            attackEventModel.setDestIpCountryCode(dstIpAreaInfo.getOrDefault("contryCode", "").toString());
            attackEventModel.setDestIpProvince(dstIpAreaInfo.getOrDefault("province", "").toString());
            attackEventModel.setDestIpProvinceCode(dstIpAreaInfo.getOrDefault("provinceCode", "").toString());
            attackEventModel.setDestIpCity(dstIpAreaInfo.getOrDefault("city", "").toString());
            attackEventModel.setDestIpCityCode(dstIpAreaInfo.getOrDefault("cityCode", "").toString());
            attackEventModel.setDestIpOperator(dstIpAreaInfo.getOrDefault("operator", "").toString());
            attackEventModel.setDestIpLongitude(dstIpAreaInfo.getOrDefault("longitude", "").toString());
            attackEventModel.setDestIpLatitude(dstIpAreaInfo.getOrDefault("latitude", "").toString());
            attackEventModel.setDestIpIdc(dstIpAreaInfo.getOrDefault("idc", "").toString());

            //目的ip企业信息
            Map<String, Object> destCorpInfo = industryHandler.handler(attackEventModel.getDestCompany());
            attackEventModel.setDestIndustryType(MapFieldUtil.getMapFieldValue(destCorpInfo, "category_name", ""));
        }
        return attackEventModel;
    }

    @Override
    public Boolean filterData(Object data) {
        AttackEventModel attackEventModel = (AttackEventModel) data;
        return StringUtils.isNotEmpty(attackEventModel.getId());
    }
}
