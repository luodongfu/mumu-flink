package com.lovecws.mumu.flink.streaming.task.ministry;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.model.gynetres.MGynetresModel;
import com.lovecws.mumu.flink.common.model.gynetres.MinistryLoopholeModel;
import com.lovecws.mumu.flink.common.model.gynetres.MinistryLoopholeResultModel;
import com.lovecws.mumu.flink.common.service.atd.CloudPlatformService;
import com.lovecws.mumu.flink.common.service.gynetres.MinistryLoopholeResultService;
import com.lovecws.mumu.flink.common.service.gynetres.MinistryLoopholeService;
import com.lovecws.mumu.flink.common.util.DateUtils;
import com.lovecws.mumu.flink.common.util.IPUtil;
import com.lovecws.mumu.flink.common.util.MD5Util;
import com.lovecws.mumu.flink.common.util.TxtUtils;
import com.lovecws.mumu.flink.streaming.common.cache.MCorpCache;
import com.lovecws.mumu.flink.streaming.common.handler.IpipnetHandler;
import com.lovecws.mumu.flink.streaming.task.AbstractStreamingTask;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;

/**
 * @Author: wucw
 * @Date: 2019-7-5 9:47
 * @Description: 部dpi数据处理
 * （kafka获取数据 -> 数据处理 -> 数据入Es -> 数据入hive）
 */
@Slf4j
@ModularAnnotation(type = "task", name = "mgynetwnt")
public class MGynetWntEventTask extends AbstractStreamingTask {

    private static final long serialVersionUID = 1L;

    private String[] storageFields;

    public MGynetWntEventTask(Map<String, Object> configMap) {
        super(configMap);
        storageFields = MapUtils.getString(configMap, "fields", "").split(",");
    }

    @Override
    public Object parseData(Object data) {
        String line = new String((byte[]) data, StandardCharsets.UTF_8);
        MGynetresModel gm = new MGynetresModel();
        TxtUtils.transforModel(line, storageFields, gm);
        //根据ip+端口计算主键
        gm.setId(MD5Util.md5(gm.getIp() + "_" + gm.getPort()));
        gm.setDs(DateUtils.formatDate(new Date(), "yyyy-MM-dd"));
        return gm;
    }

    @Override
    public Object fillData(Object data) {
        MCorpCache mCorpCache = configManager.getBean(MCorpCache.class);
        IpipnetHandler ipipnetHandler = configManager.getBean(IpipnetHandler.class);
        MinistryLoopholeService ministryLoopholeService = configManager.getBean(MinistryLoopholeService.class);
        MinistryLoopholeResultService ministryLoopholeResultService = configManager.getBean(MinistryLoopholeResultService.class);
        CloudPlatformService cloudPlatformService = configManager.getBean(CloudPlatformService.class);

        MGynetresModel gm = (MGynetresModel) data;

        // 通过ip在tb_enterprise中查询企业名称等
        long ipValue = IPUtil.ipToLong(gm.getIp());
        if (ipValue != 0) {
            // 获取云平台名称
            String name = cloudPlatformService.getCloudPlatformNameByIp(ipValue);
            gm.setCloudPlatformName(name);

            Map<String, Object> corpInfo = mCorpCache.getCorpInfo(ipValue);
            gm.setIpoperIpUnit(corpInfo.getOrDefault("corpname", "").toString());
            gm.setIpoperUsingUnitIndustryType(corpInfo.getOrDefault("industry", "").toString());
            gm.setIpoperUsingUnitLxrDzyj(corpInfo.getOrDefault("corptype", "").toString());
        }
        // 如果tb_enterprise都没有数据，就再次查询tb_enterprise_base
        if (StringUtils.isBlank(gm.getIpoperIpUnit()) && ipValue != 0) {
            Map<String, Object> corpInfo = mCorpCache.getCorpInfoFromDB(ipValue);
            gm.setIpoperIpUnit(corpInfo.getOrDefault("corpname", "").toString());
            gm.setIpoperUsingUnitIndustryType(corpInfo.getOrDefault("industry", "").toString());
            gm.setIpoperUsingUnitLxrDzyj(corpInfo.getOrDefault("corptype", "").toString());
        }

        String country = "";
        String province = "";
        String city = "";
        String countryCode = "";
        String provinceCode = "";
        String cityCode = "";
        String idc = "";
        String owner = "";
        String ipOperator = "";
        String ipLongitude = "";
        String ipLatitude = "";
        String ipArea = "";
        String areaCode = "";
        String globalRoaming = "";
        if (!StringUtils.isEmpty(gm.getIp())) {
            Map<String, Object> ipInfo = ipipnetHandler.handler(gm.getIp());
            if (ipInfo != null) {
                // ip所属国家
                country = ipInfo.get("country").toString();
                // ip所属省份
                province = ipInfo.get("province").toString();
                // ip地级市/省直辖县级行政区
                city = ipInfo.get("city").toString();
                // ip所属国家
                countryCode = ipInfo.get("internalCode").toString();
                // ip所属省份
                provinceCode = ipInfo.get("provinceCode").toString();
                // ip地级市/省直辖县级行政区
                cityCode = ipInfo.get("cityCode").toString();
                // idc段所有者
                idc = ipInfo.get("idc").toString();
                // ip段所有者
                owner = ipInfo.get("owner").toString();
                // ip所属区域
                ipArea = ipInfo.get("ipArea").toString();
                // 中国行政区划代码
                areaCode = ipInfo.get("areaCode").toString();
                // 国际区号
                globalRoaming = ipInfo.get("globalRoaming").toString();
                // ip运营商
                ipOperator = ipInfo.get("ipOperator").toString();
                // ip维度
                ipLongitude = ipInfo.get("ipLongitude").toString();
                // ip经度
                ipLatitude = ipInfo.get("ipLatitude").toString();
            }
        }

        gm.setIpoperIdc(idc);
        gm.setIpoperCity(cityCode);
        gm.setIpoperCityName(city);
        gm.setIpoperProvince(provinceCode);
        gm.setIpoperProvinceName(province);
        gm.setIpoperCountryCode(countryCode);
        gm.setIpoperCountryName(country);
        gm.setIpoperOperatorName(ipOperator);
//        gm.setIpoperIpUnit(owner);
        gm.setIpoperLongitude(ipLongitude);
        gm.setIpoperLatitude(ipLatitude);

        try {
            if (gm.getLoophole() != null && gm.getLoophole()) {
                String uuidStr = gm.getUuid();
                String[] uuids = uuidStr.split(",");
                for (String uuid : uuids) {
                    LambdaQueryWrapper<MinistryLoopholeModel> queryWrapper = new QueryWrapper<MinistryLoopholeModel>().lambda();
                    queryWrapper.eq(MinistryLoopholeModel::getUuid, uuid);
                    MinistryLoopholeModel model = ministryLoopholeService.getOne(queryWrapper);
                    if (model == null) {
                        continue;
                    }
                    String jsonString = JSON.toJSONString(model);
                    // JSON串转用户组对象
                    MinistryLoopholeResultModel result = JSON.parseObject(jsonString, MinistryLoopholeResultModel.class);
                    result.setIp(gm.getIp());
                    result.setPort(gm.getPort());
                    result.setDeviceType(gm.getDeviceSecondaryNameCn());
                    result.setDeviceVersion(gm.getVersion());
                    result.setProtocol(gm.getProtocol());
                    result.setServiceType(gm.getService());
                    result.setServiceVersion(gm.getServiceVersion());
                    result.setOs(gm.getOs());
                    result.setDeviceVendor(gm.getVendor());
                    result.setFindTime(gm.getUpdateTime());

                    result.setIpUseunit(gm.getIpoperIpUnit());
                    result.setIndustryType(gm.getIpoperUsingUnitIndustryType());
                    result.setIpUseunitNature(gm.getIpoperUsingUnitLxrDzyj());

                    result.setIpOperator(gm.getIpoperOperatorName());
                    result.setCountry(gm.getIpoperCountryCode());
                    result.setProvince(gm.getIpoperProvince());
                    result.setCity(gm.getIpoperCity());
//            		result.setIpUseunit(gm.getIpoperIpUnit());
                    result.setIpArea(ipArea);
                    result.setIpAreacode(areaCode);
                    result.setIpGlobalroaming(globalRoaming);

                    // 入库时间
                    result.setInsertTime(new Date());
                    // 云平台名称
                    result.setCloudPlatformName(gm.getCloudPlatformName());

                    ministryLoopholeResultService.save(result);
                }
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        }
        return gm;
    }

    @Override
    public Boolean filterData(Object data) {
        return true;
    }
}
