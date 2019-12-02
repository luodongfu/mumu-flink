package com.lovecws.mumu.flink.streaming.task.ministry;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.model.atd.MAtdEventModel;
import com.lovecws.mumu.flink.common.service.atd.CloudPlatformService;
import com.lovecws.mumu.flink.common.util.DateUtils;
import com.lovecws.mumu.flink.common.util.IPUtil;
import com.lovecws.mumu.flink.common.util.MD5Util;
import com.lovecws.mumu.flink.common.util.TxtUtils;
import com.lovecws.mumu.flink.streaming.common.cache.MCorpCache;
import com.lovecws.mumu.flink.streaming.common.cache.SyslogDictCache;
import com.lovecws.mumu.flink.streaming.common.handler.IpipnetHandler;
import com.lovecws.mumu.flink.streaming.task.AbstractStreamingTask;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @Author: wucw
 * @Date: 2019-7-5 9:47
 * @Description: 部dpi数据处理
 * （kafka获取数据 -> 数据处理 -> 数据入Es -> 数据入hive）
 */
@Slf4j
@ModularAnnotation(type = "task", name = "matdhajx")
public class MAtdHajxEventTask extends AbstractStreamingTask {

    private static final long serialVersionUID = 1L;
    //被攻击方事件类型(目的ip攻击源ip事件类型)
    private static final List<String> ATTACKED_EVENT_TYPES = Arrays.asList("1", "6", "3", "13", "15", "16");
    //被攻击方事件二级分类(目的ip攻击源ip事件类型)
    private static final List<String> ATTACKED_EVENT_SUBCATEGORY = Arrays.asList("特洛伊木马通信", "恶意加密流量");

    private int srcIpFind = 0;
    private int dstIpFind = 0;
    private IpipnetHandler ipipnetHandler;

    private String[] storageFields;

    public MAtdHajxEventTask(Map<String, Object> configMap) {
        super(configMap);
        ipipnetHandler = new IpipnetHandler();
        storageFields = MapUtils.getString(configMap, "fields", "").split(",");
    }

    @Override
    public Object parseData(Object data) {
        MAtdEventModel atdModel = new MAtdEventModel();
        String line = new String((byte[]) data, StandardCharsets.UTF_8);
        TxtUtils.transforModel(line, storageFields, FieldUtils.getAllFields(atdModel.getClass()), atdModel, "yyyy-MM-dd HH:mm:ss", "\\|");

        atdModel.setCreateTime(new Date());

        Date timestamp = atdModel.getTimestamp();
        if (timestamp == null) timestamp = new Date();
        atdModel.setDs(DateUtils.formatDate(timestamp, "yyyy-MM-dd"));
        //将整行的值计算md5设置为id
        atdModel.setId(MD5Util.md5(line));
        return atdModel;
    }

    @Override
    public Object fillData(Object data) {
        CloudPlatformService cloudPlatformService = configManager.getBean(CloudPlatformService.class);
        SyslogDictCache syslogDictCache = configManager.getBean(SyslogDictCache.class);
        MAtdEventModel atdModel = (MAtdEventModel) data;

        //源ip
        String srcIp = atdModel.getSrcIp();
        if (StringUtils.isNotEmpty(srcIp)) {
            atdModel.setSrcIpValue(IPUtil.ipToLong(srcIp));
            atdModel.setSrcIpType(IPUtil.ipType(srcIp));
            //源ip地市信息
            Map<String, Object> srcIpAreaInfo = ipipnetHandler.handler(atdModel.getSrcIp());
            atdModel.setSrcIpCountryFromIpNet(srcIpAreaInfo.getOrDefault("contry", "").toString());
            atdModel.setSrcIpCountryCodeFromIpNet(srcIpAreaInfo.getOrDefault("contryCode", "").toString());
            atdModel.setSrcIpProvinceFromIpNet(srcIpAreaInfo.getOrDefault("province", "").toString());
            atdModel.setSrcIpProvinceCodeFromIpNet(srcIpAreaInfo.getOrDefault("provinceCode", "").toString());
            atdModel.setSrcIpCityFromIpNet(srcIpAreaInfo.getOrDefault("city", "").toString());
            atdModel.setSrcIpCityCodeFromIpNet(srcIpAreaInfo.getOrDefault("cityCode", "").toString());
        }
        //目的ip
        String dstIp = atdModel.getDstIp();
        if (StringUtils.isNotEmpty(dstIp)) {
            atdModel.setDstIpValue(IPUtil.ipToLong(dstIp));
            atdModel.setDstIpType(IPUtil.ipType(dstIp));

            //目的ip地市信息
            Map<String, Object> dstIpAreaInfo = ipipnetHandler.handler(atdModel.getDstIp());
            atdModel.setDstIpCountryFromIpNet(dstIpAreaInfo.getOrDefault("contry", "").toString());
            atdModel.setDstIpCountryCodeFromIpNet(dstIpAreaInfo.getOrDefault("contryCode", "").toString());
            atdModel.setDstIpProvinceFromIpNet(dstIpAreaInfo.getOrDefault("province", "").toString());
            atdModel.setDstIpProvinceCodeFromIpNet(dstIpAreaInfo.getOrDefault("provinceCode", "").toString());
            atdModel.setDstIpCityFromIpNet(dstIpAreaInfo.getOrDefault("city", "").toString());
            atdModel.setDstIpCityCodeFromIpNet(dstIpAreaInfo.getOrDefault("cityCode", "").toString());
        }

        //威胁等级映射关系
        int severity = atdModel.getSeverity();
        atdModel.setSeverityCn(syslogDictCache.getSyslogSeverity(String.valueOf(severity)));

        //组合主机字段信息
        atdModel.setSrcIpPort(atdModel.getSrcIp() + "_" + atdModel.getSrcPort());
        atdModel.setDstIpPort(atdModel.getDstIp() + "_" + atdModel.getDstPort());
        atdModel.setIpJoint(atdModel.getSrcIp() + "_" + atdModel.getDstIp());
        atdModel.setIpPortJoint(atdModel.getSrcIpPort() + "_" + atdModel.getDstIpPort());

        //攻击方 被攻击方
        String eventTypeId = atdModel.getEventTypeId();
        //目的ip是攻击源ip事件类型
        if (ATTACKED_EVENT_TYPES.contains(eventTypeId)) {
            atdModel.setAttackIp(atdModel.getDstIp());
            atdModel.setAttackPort(atdModel.getDstPort());
            atdModel.setAttackCountry(atdModel.getDstIpCountry());
            atdModel.setAttackProvince(atdModel.getDstIpProvince());
            atdModel.setAttackCity(atdModel.getDstIpCity());
            atdModel.setAttackCorpName(atdModel.getCorpName());
            atdModel.setAttackIndustry(atdModel.getIndustry());
            atdModel.setAttackGeoloc(atdModel.getDstIpGeoloc());

            atdModel.setAttackedIp(atdModel.getSrcIp());
            atdModel.setAttackedPort(atdModel.getSrcPort());
            atdModel.setAttackedCountry(atdModel.getSrcIpCountry());
            atdModel.setAttackedProvince(atdModel.getSrcIpProvince());
            atdModel.setAttackedCity(atdModel.getSrcIpCity());
            atdModel.setAttackedCorpName(atdModel.getSrcCorpName());
            atdModel.setAttackedIndustry(atdModel.getSrcIndustry());
            atdModel.setAttackedGeoloc(atdModel.getSrcIpGeoloc());
        }
        //源ip攻击目的ip事件类型
        else {
            atdModel.setAttackIp(atdModel.getSrcIp());
            atdModel.setAttackPort(atdModel.getSrcPort());
            atdModel.setAttackCountry(atdModel.getSrcIpCountry());
            atdModel.setAttackProvince(atdModel.getSrcIpProvince());
            atdModel.setAttackCity(atdModel.getSrcIpCity());
            atdModel.setAttackCorpName(atdModel.getSrcCorpName());
            atdModel.setAttackIndustry(atdModel.getSrcIndustry());

            atdModel.setAttackedIp(atdModel.getDstIp());
            atdModel.setAttackedPort(atdModel.getDstPort());
            atdModel.setAttackedCountry(atdModel.getDstIpCountry());
            atdModel.setAttackedProvince(atdModel.getDstIpProvince());
            atdModel.setAttackedCity(atdModel.getDstIpCity());
            atdModel.setAttackedCorpName(atdModel.getCorpName());
            atdModel.setAttackedIndustry(atdModel.getIndustry());
        }

        // 获取云平台名称
        String attackedIp = atdModel.getAttackedIp();
        if (StringUtils.isNotBlank(attackedIp)) {
            String name = cloudPlatformService.getCloudPlatformNameByIp(IPUtil.ipToLong(attackedIp));
            atdModel.setCloudPlatformName(name);
        }

        // 补充源ip、目的ip、攻击ip、被攻击ip的企业名臣、企业性质等(从tb_enterprise)
        supplyCorp(atdModel, "enterprise");
        // 补充源ip、目的ip、攻击ip、被攻击ip的企业名臣、企业性质等(从tb_enterprise_base)
        supplyCorp(atdModel, "enterprise_base");

        return atdModel;
    }

    private void supplyCorp(MAtdEventModel atd, String type) {
        MCorpCache mCorpCache = configManager.getBean(MCorpCache.class);
        try {
            String srcCorpType = null, dstCorpType = null;
            // 重新查找源ip的企业名称
            if (StringUtils.isBlank(atd.getSrcCorpName()) && StringUtils.isNotBlank(atd.getSrcIp())) {
                Map<String, Object> srcCorpInfo = null;
                if ("enterprise".equals(type)) {
                    srcCorpInfo = mCorpCache.getCorpInfo(atd.getSrcIpValue());
                } else if ("enterprise_base".equals(type)) {
                    srcCorpInfo = mCorpCache.getCorpInfoFromDB(atd.getSrcIpValue());

                    // 统计使用
                    if (StringUtils.isNotBlank(srcCorpInfo.getOrDefault("corpname", "").toString())) {
                        srcIpFind++;
                    }
                }
                atd.setSrcCorpName(srcCorpInfo.getOrDefault("corpname", "").toString());
                atd.setSrcIndustry(srcCorpInfo.getOrDefault("industry", "").toString());
                srcCorpType = srcCorpInfo.getOrDefault("corptype", "").toString();
            }
            // 重新查找目的ip的企业名称
            if (StringUtils.isBlank(atd.getCorpName()) && StringUtils.isNotBlank(atd.getDstIp())) {
                Map<String, Object> corpInfo = null;
                if ("enterprise".equals(type)) {
                    corpInfo = mCorpCache.getCorpInfo(atd.getDstIpValue());
                } else if ("enterprise_base".equals(type)) {
                    corpInfo = mCorpCache.getCorpInfoFromDB(atd.getDstIpValue());

                    // 统计使用
                    if (StringUtils.isNotBlank(corpInfo.getOrDefault("corpname", "").toString())) {
                        dstIpFind++;
                    }
                }
                atd.setCorpName(corpInfo.getOrDefault("corpname", "").toString());
                atd.setIndustry(corpInfo.getOrDefault("industry", "").toString());
                dstCorpType = corpInfo.getOrDefault("corptype", "").toString();
            }
            // 攻击方 被攻击方
            String eventTypeId = atd.getEventTypeId();
            String subCategory = atd.getCategory();
            // 目的ip是攻击源ip事件类型
            if (ATTACKED_EVENT_TYPES.contains(eventTypeId) || ATTACKED_EVENT_SUBCATEGORY.contains(subCategory)) {
                atd.setAttackCorpName(atd.getCorpName());
                atd.setAttackIndustry(atd.getIndustry());
                if ((StringUtils.isNotBlank(dstCorpType))) {
                    atd.setAttackCorpType(dstCorpType);
                }

                atd.setAttackedCorpName(atd.getSrcCorpName());
                atd.setAttackedIndustry(atd.getSrcIndustry());
                if ((StringUtils.isNotBlank(srcCorpType))) {
                    atd.setAttackedCorpType(srcCorpType);
                }
            }
            // 源ip攻击目的ip事件类型
            else {
                atd.setAttackCorpName(atd.getSrcCorpName());
                atd.setAttackIndustry(atd.getSrcIndustry());
                if ((StringUtils.isNotBlank(srcCorpType))) {
                    atd.setAttackCorpType(srcCorpType);
                }

                atd.setAttackedCorpName(atd.getCorpName());
                atd.setAttackedIndustry(atd.getIndustry());
                if ((StringUtils.isNotBlank(dstCorpType))) {
                    atd.setAttackedCorpType(dstCorpType);
                }
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        }
    }

    @Override
    public Boolean filterData(Object data) {
        return true;
    }
}
