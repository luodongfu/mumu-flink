package com.lovecws.mumu.flink.streaming.task;

import com.alibaba.fastjson.JSON;
import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.model.gynetres.GynetresModel;
import com.lovecws.mumu.flink.common.model.gynetres.HostLoopholeModel;
import com.lovecws.mumu.flink.common.model.loophole.CnvdModel;
import com.lovecws.mumu.flink.common.service.gynetres.HostLoopholeService;
import com.lovecws.mumu.flink.common.service.loophole.CnvdService;
import com.lovecws.mumu.flink.common.util.*;
import com.lovecws.mumu.flink.streaming.common.cache.CnvdLoopholeCache;
import com.lovecws.mumu.flink.streaming.common.handler.IndustryHandler;
import com.lovecws.mumu.flink.streaming.common.handler.IpbaseHandler;
import com.lovecws.mumu.flink.streaming.common.handler.IpipnetHandler;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;

import java.net.URLDecoder;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @program: act-able
 * @description: 资产探测任务, 将资产数据进行ip归属地匹配、基础资源接口匹配、行业分类匹配、漏洞匹配
 * 数据存储: es(按照ip+端口进行清洗之后的结果)，hive(明细表)
 * @author: 甘亮
 * @create: 2019-06-05 18:01
 **/
@Slf4j
@ModularAnnotation(type = "task", name = "gynetres")
public class GynetresStreamingTask extends AbstractStreamingTask {

    private IpipnetHandler ipipnetHandler;
    private IndustryHandler industryHandler;
    private IpbaseHandler ipbaseHandler;

    public GynetresStreamingTask(Map<String, Object> configMap) {
        super(configMap);

        ipipnetHandler = new IpipnetHandler();
        industryHandler = new IndustryHandler();
        ipbaseHandler = new IpbaseHandler();
    }

    @Override
    public Object parseData(Object line) {
        Map<String, Object> resultMap = new HashMap<>();
        Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(AvroUtil.getSchema(GynetresModel.class));
        try {
            GenericRecord genericRecord = recordInjection.invert((byte[]) line).get();
            genericRecord.getSchema().getFields().forEach(field -> {
                Object o = null;
                try {
                    o = genericRecord.get(field.pos());
                    if (o != null) {
                        resultMap.put(field.name(), URLDecoder.decode(String.valueOf(o)));
                    }
                } catch (Exception ex) {
                    resultMap.put(field.name(), String.valueOf(o));
                }
            });

            return JSON.parseObject(JSON.toJSONString(resultMap), GynetresModel.class);
        } catch (Exception e) {
            log.error(e.getLocalizedMessage() + "[" + JSON.toJSONString(resultMap) + "]", e);
        }
        return new GynetresModel();
    }

    @Override
    public Object fillData(Object data) {
        GynetresModel gynetresModel = (GynetresModel) data;
        try {
            //过滤ip为空的资产数据
            if (StringUtils.isEmpty(gynetresModel.getIp())) return gynetresModel;
            if (StringUtils.isEmpty(gynetresModel.getTaskId())) return gynetresModel;

            gynetresModel.setTaskId(gynetresModel.getTaskId().replaceAll("\\D+", ""));

            //匹配ipipnet 获取运营商信息
            Map<String, Object> ipipnetMap = ipipnetHandler.handler(gynetresModel.getIp());
            gynetresModel.setIpoperLatitude(MapFieldUtil.getMapFieldValue(ipipnetMap, "latitude", ""));
            gynetresModel.setIpoperLongitude(MapFieldUtil.getMapFieldValue(ipipnetMap, "longitude", ""));
            gynetresModel.setIpoperIdc(MapFieldUtil.getMapFieldValue(ipipnetMap, "idc", ""));
            gynetresModel.setIpoperOperatorName(MapFieldUtil.getMapFieldValue(ipipnetMap, "operator", ""));

            gynetresModel.setIpoperCountryName(MapFieldUtil.getMapFieldValue(ipipnetMap, "contry", ""));
            gynetresModel.setIpoperCountryCode(MapFieldUtil.getMapFieldValue(ipipnetMap, "contryCode", ""));
            gynetresModel.setIpoperProvinceName(MapFieldUtil.getMapFieldValue(ipipnetMap, "province", ""));

            gynetresModel.setIpoperCityName(MapFieldUtil.getMapFieldValue(ipipnetMap, "city", ""));

            //匹配基础资源 获取企业信息
            Map<String, Object> ipbaseMap = ipbaseHandler.handler(gynetresModel.getIp());
            gynetresModel.setIpoperRelatedFields(MapFieldUtil.getMapFieldValue(ipbaseMap, "related_fields", ""));
            gynetresModel.setIpoperIddCode(MapFieldUtil.getMapFieldValue(ipbaseMap, "idd_code", ""));
            //先以ipipnet为准备 如果不存在 则从基础资源获取
            gynetresModel.setIpoperProvince(MapFieldUtil.getMapFieldValue(ipipnetMap, "provinceCode", MapFieldUtil.getMapFieldValue(ipbaseMap, "province", "")));
            gynetresModel.setIpoperCity(MapFieldUtil.getMapFieldValue(ipipnetMap, "cityCode", MapFieldUtil.getMapFieldValue(ipbaseMap, "city", "")));

            gynetresModel.setIpoperDetailArea(MapFieldUtil.getMapFieldValue(ipbaseMap, "addr", ""));
            gynetresModel.setIpoperIpUnit(MapFieldUtil.getMapFieldValue(ipbaseMap, "usingUnitName", ""));

            gynetresModel.setIpoperUsingUnitLinkman(MapFieldUtil.getMapFieldValue(ipbaseMap, "usingUnitLinkman", ""));
            gynetresModel.setIpoperUsingUnitTel(MapFieldUtil.getMapFieldValue(ipbaseMap, "usingUnitTel", ""));
            gynetresModel.setIpoperUsingUnitType(MapFieldUtil.getMapFieldValue(ipbaseMap, "usingUnitType", ""));
            gynetresModel.setIpoperUsingUnitIndustry(MapFieldUtil.getMapFieldValue(ipbaseMap, "usingUnitIndustryType", ""));

            //根据企业信息 获取行业分类信息
            Map<String, Object> industryMap = industryHandler.handler(gynetresModel.getIpoperIpUnit());
            gynetresModel.setIpoperUsingUnitIndustryType(MapFieldUtil.getMapFieldValue(industryMap, "category_smallname", ""));

            //根据ip+端口计算主键
            gynetresModel.setId(MD5Util.md5(gynetresModel.getIp() + "_" + gynetresModel.getPort()));
            gynetresModel.setCounter(1L);

            gynetresModel.setProcessTime(new Date());

            gynetresModel.setCreateTime(new Date());

            gynetresModel.setUpdateTime(gynetresModel.getCreateTime());
            gynetresModel.setDs(DateUtils.formatDate(gynetresModel.getCreateTime(), "yyyy-MM-dd"));

            //调整htmlContent的长度
            String htmlContent = gynetresModel.getHtmlContent();
            if (htmlContent == null) htmlContent = "";
            htmlContent = EscapeUtil.escape(htmlContent);
            if (htmlContent.length() > 16000) {
                htmlContent = htmlContent.substring(0, 16000);
            }
            gynetresModel.setHtmlContent(htmlContent);

            String res = gynetresModel.getRes();
            if (res == null) res = "";
            res = EscapeUtil.escape(res);
            if (res.length() > 16000) {
                res = res.substring(0, 16000);
            }
            gynetresModel.setRes(res);

            //match cnvd loophole
            matchCnvdLoophole(gynetresModel);
            return gynetresModel;
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage() + "[" + JSON.toJSONString(data) + "]", ex);
        }
        return gynetresModel;
    }

    /**
     * 匹配cnvd漏洞
     *
     * @param gynetresModel 资产探测实体
     */
    public void matchCnvdLoophole(GynetresModel gynetresModel) {
        CnvdService cnvdService = new CnvdService();
        CnvdLoopholeCache cnvdLoopholeCache = new CnvdLoopholeCache();
        HostLoopholeService hostLoopholeService = new HostLoopholeService();
        try {
            List<CnvdModel> cnvdModels = cnvdLoopholeCache.queryCnvdRefectProduct(gynetresModel.getNmapProduct());
            cnvdModels = cnvdService.matchCnvdLoophole(cnvdModels, gynetresModel.getNmapProduct(), gynetresModel.getNmapVersion());
            cnvdModels.forEach(cnvdModel -> {
                HostLoopholeModel hostLoopholeModel = new HostLoopholeModel();
                hostLoopholeModel.setTaskId(Integer.parseInt(gynetresModel.getTaskId()));
                hostLoopholeModel.setTaskInstanceId(gynetresModel.getTaskInstanceId());
                hostLoopholeModel.setIp(gynetresModel.getIp());
                hostLoopholeModel.setPort(gynetresModel.getPort());
                hostLoopholeModel.setIpOperator(gynetresModel.getIpoperOperatorName());
                hostLoopholeModel.setIpAreacode(gynetresModel.getIpoperCity());
                hostLoopholeModel.setIpArea(gynetresModel.getIpoperCityName());
                hostLoopholeModel.setIpGlobalroaming(gynetresModel.getIpoperIddCode());
                hostLoopholeModel.setDeviceType(gynetresModel.getDeviceSecondaryNameCn());
                hostLoopholeModel.setLoopholeType(cnvdModel.getSoftStyle());
                hostLoopholeModel.setDeviceName(gynetresModel.getNmapProduct());
                hostLoopholeModel.setDamageLevel(cnvdModel.getServerity());
                hostLoopholeModel.setDataSource(gynetresModel.getDataSource());
                hostLoopholeModel.setProtocol(gynetresModel.getProtocol());
                hostLoopholeModel.setProtocolType(gynetresModel.getService());
                hostLoopholeModel.setThreat(cnvdModel.getThread());
                hostLoopholeModel.setDeviceVersion(gynetresModel.getNmapVersion());
                hostLoopholeModel.setCnvdNo(cnvdModel.getCnvdNo());
                hostLoopholeModel.setLoopholeDescription(cnvdModel.getDescription());
                hostLoopholeModel.setSfVersion(gynetresModel.getModuleNum());
                hostLoopholeModel.setProtocolVersion(gynetresModel.getServiceVersion());
                hostLoopholeModel.setEventId(cnvdModel.getCnvdNo());
                hostLoopholeModel.setCreateTime(gynetresModel.getCreateTime());
                hostLoopholeModel.setServiceType(gynetresModel.getDevicePrimaryName());
                hostLoopholeModel.setServiceTypeCn(gynetresModel.getDevicePrimaryNameCn());
                hostLoopholeModel.setEventSource(0);
                hostLoopholeModel.setIpUseunit(gynetresModel.getIpoperIpUnit());
                hostLoopholeModel.setIndustry(gynetresModel.getIpoperUsingUnitIndustry());
                hostLoopholeModel.setIndustryType(gynetresModel.getIpoperUsingUnitIndustryType());
                hostLoopholeModel.setVendor(gynetresModel.getVendor());
                hostLoopholeModel.setVendorSource(gynetresModel.getVendorSource());
                hostLoopholeService.save(hostLoopholeModel);
            });
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage());
            ex.printStackTrace();
        }
    }

    @Override
    public Boolean filterData(Object data) {
        GynetresModel gynetresModel = (GynetresModel) data;
        return StringUtils.isNotEmpty(gynetresModel.getIp());
    }
}
