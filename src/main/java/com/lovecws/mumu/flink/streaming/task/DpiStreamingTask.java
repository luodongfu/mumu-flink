package com.lovecws.mumu.flink.streaming.task;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.config.ConfigProperties;
import com.lovecws.mumu.flink.common.model.dpi.CorpInfo;
import com.lovecws.mumu.flink.common.model.dpi.DpiModel;
import com.lovecws.mumu.flink.common.model.dpi.ProtocolDevice;
import com.lovecws.mumu.flink.common.redis.JedisUtil;
import com.lovecws.mumu.flink.common.service.dpi.CorpInfoService;
import com.lovecws.mumu.flink.common.service.dpi.ProtocolDeviceService;
import com.lovecws.mumu.flink.common.util.DateUtils;
import com.lovecws.mumu.flink.common.util.IPUtil;
import com.lovecws.mumu.flink.streaming.common.constants.DpiConstant;
import com.lovecws.mumu.flink.streaming.common.handler.IpipnetHandler;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.JedisCommands;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * @Author: lyc
 * @Date: 2019-6-26 11:34
 * @Description: dpi数据处理
 * （kafka获取数据 -> 数据处理 -> 数据入Es -> 数据入hive）
 */
@Slf4j
@ModularAnnotation(type = "task", name = "dpi")
public class DpiStreamingTask extends AbstractStreamingTask {

    private IpipnetHandler ipipnetHandler;
    private CorpInfoService corpInfoService;

    // 时间格式
    private static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    //协议的匹配规则，不能以特殊字符开头
    private static String serviceRep = "^[0-9a-zA-z].*";

    //redis缓存的失效时间，此处为一天
    private static int redisExpire = 60 * 60 * 24;


    public DpiStreamingTask(Map<String, Object> configMap) {
        super(configMap);
        ipipnetHandler = new IpipnetHandler();
        corpInfoService = new CorpInfoService();
    }

    @Override
    public Object parseData(Object data) {
        DpiModel dpiModel = new DpiModel();
        String line = new String((byte[]) data, StandardCharsets.UTF_8);
        //获取IPIPNET
        String[] temps = line.split(DpiConstant.DPI_DECRY_SPECIAL_CHARACTERS);
        // 1、数据校验
        // 如果读取的这一行数据没有这固定的9个值、目的IP为空或者协议名为空，都视为废数据
        if (temps.length < 10) {
            return dpiModel;
        }
        String srcIp = temps[0];
        String destIp = temps[1];
        String srcPort = temps[2];
        String destPort = temps[3];
        String service = temps[4];
        String upPackNum = temps[5];
        String upByteNum = temps[6];
        String downPackNum = temps[7];
        String downByteNum = temps[8];
        String createTime = temps[9];
        if (srcIp.isEmpty() || destIp.isEmpty() || srcPort.isEmpty() || destPort.isEmpty()) {
            return dpiModel;
        }
        if (service.isEmpty() || service.length() > 30 || !service.matches(serviceRep)) {
            return dpiModel;
        }

        // 2、数据格式化
        Long srcIpLong = IPUtil.ipToLong(srcIp);


        dpiModel.setPrimaryId(srcIpLong + service + srcPort);
        dpiModel.setSrcIp(srcIp);
        dpiModel.setDestIp(destIp);
        dpiModel.setSrcPort(srcPort);
        dpiModel.setDestPort(destPort);
        dpiModel.setService(service);
        dpiModel.setUpPackNum(upPackNum);
        dpiModel.setUpByteNum(upByteNum);
        dpiModel.setDownPackNum(downPackNum);
        dpiModel.setDownByteNum(downByteNum);

        if (!isValidDate(createTime)) {
            log.error("[时间格式不对]---" + line);
            dpiModel.setCreateTime(new Date());
        } else {
            try {
                dpiModel.setCreateTime(formatter.parse(createTime));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        //设置分区时间
        dpiModel.setDs(DateUtils.formatDate(dpiModel.getCreateTime(), "yyyy-MM-dd"));
        //设置ID，作为hive id
        dpiModel.setId(UUID.randomUUID().toString().replace("-", ""));

        //处理dpi日志动态字段逻辑
        StringBuffer dynamicField = new StringBuffer();
        //定义填充字段
        String fillField = "";
        for (int i = 10; i < temps.length; i++) {
            String temp = temps[i];
            if (StringUtils.isBlank(temp)) {
                continue;
            } else {
                dynamicField.append(temp).append("|||");
            }

            String[] tempMap = temp.split(":::");
            //填充字段动态赋值
            fillField = tempMap.length == 1 ? "" : tempMap[1];

            if (tempMap[0].equalsIgnoreCase("url")) {
                dpiModel.setUrl(fillField);
            }

            if (tempMap[0].equalsIgnoreCase("data") || tempMap[0].equalsIgnoreCase("Data")) {
                dpiModel.setData(fillField);
            }

            //根据dpi日志字段填充设备信息
            if (tempMap[0].equalsIgnoreCase(DpiConstant.DPI_FIELD_DEVICEINFO_DEVICEID)) {
                dpiModel.setDeviceinfoDeviceid(fillField);
            }
            if (tempMap[0].equalsIgnoreCase(DpiConstant.DPI_FIELD_DEVICEINFO_SN)) {
                dpiModel.setDeviceinfoSn(fillField);
            }
            if (tempMap[0].equalsIgnoreCase(DpiConstant.DPI_FIELD_DEVICEINFO_MAC)) {
                dpiModel.setDeviceinfoMac(fillField);
            }
            if (tempMap[0].equalsIgnoreCase(DpiConstant.DPI_FIELD_DEVICEINFO_MD5)) {
                dpiModel.setDeviceinfoMd5(fillField);
            }
            if (tempMap[0].equalsIgnoreCase(DpiConstant.DPI_FIELD_DEVICEINFO_OS)) {
                dpiModel.setDeviceinfoOs(fillField);
            }
            if (tempMap[0].equalsIgnoreCase(DpiConstant.DPI_FIELD_DEVICEINFO_IP)) {
                dpiModel.setDeviceinfoIp(fillField);
            }
            if (tempMap[0].equalsIgnoreCase(DpiConstant.DPI_FIELD_DEVICEINFO_NUMBER)) {
                dpiModel.setDeviceinfoNumber(fillField);
            }
            if (tempMap[0].equalsIgnoreCase(DpiConstant.DPI_FIELD_DEVICEINFO_SOFTVERSION) || tempMap[0].equalsIgnoreCase("softVer")) {
                dpiModel.setDeviceinfoSoftVersion(fillField);
            }
            if (tempMap[0].equalsIgnoreCase(DpiConstant.DPI_FIELD_DEVICEINFO_FIRMWAREVERSION)) {
                dpiModel.setDeviceinfoFirmwareVersion(fillField);
            }
        }
        //设置动态字段
        dpiModel.setDynamicField(dynamicField.toString());

        return dpiModel;
    }

    @Override
    public Object fillData(Object object) {
        JedisCommands jedis = JedisUtil.getJedis();
        DpiModel dpiModel = (DpiModel) object;
        try {
            String srcIp = dpiModel.getSrcIp();
            String destIp = dpiModel.getDestIp();

            Long srcIpLong = IPUtil.ipToLong(srcIp);
            Long dstIpLong = IPUtil.ipToLong(destIp);


            // 3、数据填充
            // 3.1、填充使用单位和行业数据
            String srcIpKey = DpiConstant.CORPNAME_INDUSTRY_CACHE + srcIpLong % 24;
            String destIpKey = DpiConstant.CORPNAME_INDUSTRY_CACHE + dstIpLong % 24;

            Object srcByte = jedis.hget(srcIpKey, srcIp);
            if (srcByte == null) {
                LambdaQueryWrapper<CorpInfo> queryWrapper = new QueryWrapper<CorpInfo>().lambda();
                queryWrapper.le(CorpInfo::getBeginIpValue, srcIpLong).ge(CorpInfo::getEndIpValue, srcIpLong);
                CorpInfo srcInfo = corpInfoService.getOne(queryWrapper);

                String cacheData = null;
                if (srcInfo != null) {
                    dpiModel.setSrcCorpName(srcInfo.getCorpName());
                    dpiModel.setSrcIndustry(srcInfo.getIndustry());

                    // 缓存数据
                    cacheData = srcInfo.getCorpName() + "=" + srcInfo.getIndustry();
                } else {
                    cacheData = "=";
                }
                jedis.hset(srcIpKey, srcIp, cacheData);
                jedis.expire(srcIpKey, redisExpire);
            } else {
                String[] cacheData = srcByte.toString().split("=");

                dpiModel.setSrcCorpName(cacheData.length > 0 ? cacheData[0] : "");
                dpiModel.setSrcIndustry(cacheData.length > 1 ? cacheData[1] : "");
            }

            Object destByte = jedis.hget(destIpKey, destIp);
            if (destByte == null) {
                LambdaQueryWrapper<CorpInfo> queryWrapper = new QueryWrapper<CorpInfo>().lambda();
                queryWrapper.le(CorpInfo::getBeginIpValue, dstIpLong).ge(CorpInfo::getEndIpValue, dstIpLong);
                CorpInfo destInfo = corpInfoService.getOne(queryWrapper);

                String cacheData = null;
                if (destInfo != null) {
                    dpiModel.setDestCorpName(destInfo.getCorpName());
                    dpiModel.setDestIndustry(destInfo.getIndustry());

                    // 缓存数据
                    cacheData = destInfo.getCorpName() + "=" + destInfo.getIndustry();
                } else {
                    cacheData = "=";
                }
                jedis.hset(destIpKey, destIp, cacheData);
                jedis.expire(destIpKey, redisExpire);
            } else {
                String[] cacheData = destByte.toString().split("=");

                dpiModel.setDestCorpName(cacheData.length > 0 ? cacheData[0] : "");
                dpiModel.setDestIndustry(cacheData.length > 1 ? cacheData[1] : "");
            }

            // 3.2、填充设备类型
            Object url = dpiModel.getUrl();
            Object data = dpiModel.getData();
            String srcPort = dpiModel.getSrcPort();
            String destPort = dpiModel.getDestPort();
            String service = dpiModel.getService();
            if (srcPort.equals("80") || srcPort.equals("443") || destPort.equals("80") || destPort.equals("443")) {
                if (this.fillDeviceinfo(dpiModel, url, new Integer(2), DpiConstant.ASSET_FEATURES_CACHE)) {
                } else if (this.fillDeviceinfo(dpiModel, data, new Integer(3), DpiConstant.ASSET_FEATURES_CACHE)) {
                } else {
                }
            } else {
                if (this.fillDeviceinfo(dpiModel, service, new Integer(1), DpiConstant.ASSET_FEATURES_CACHE)) {
                } else if (this.fillDeviceinfo(dpiModel, url, new Integer(2), DpiConstant.ASSET_FEATURES_CACHE)) {
                } else if (this.fillDeviceinfo(dpiModel, data, new Integer(3), DpiConstant.ASSET_FEATURES_CACHE)) {
                } else {
                }
            }


            // 3.3、填充ipipnet信息
            //定义源IP标识
            Boolean srcIpFlag = true;
            if (IPUtil.ipType(srcIp).equals("ipv4")) {
                this.fillIpInfo(dpiModel, ipipnetHandler, srcIp, srcIpFlag);
            }
            if (IPUtil.ipType(destIp).equals("ipv4")) {
                this.fillIpInfo(dpiModel, ipipnetHandler, destIp, !srcIpFlag);
            }

            dpiModel.setUploadProvinceCode(ConfigProperties.getString("defaults.provice.code"));
            dpiModel.setUploadProvinceName(ConfigProperties.getString("defaults.provice.name"));
            dpiModel.setDataSource(ConfigProperties.getString("defaults.provice.source"));

        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage());
            JedisUtil.close(jedis);
        }
        return dpiModel;
    }

    @Override
    public Boolean filterData(Object data) {
        DpiModel dpiModel = (DpiModel) data;
        return StringUtils.isNotEmpty(dpiModel.getId());
    }

    /**
     * 验证字符串是否为指定时间格式
     *
     * @param date
     * @return
     */
    public boolean isValidDate(String date) {
        try {
            formatter.setLenient(false);
            formatter.parse(date);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public Boolean fillDeviceinfo(DpiModel dpiModel, Object fieldName, Integer fieldType,
                                  String cacheName) {
        Boolean isFill = Boolean.FALSE;
        if (fieldName == null || StringUtils.isBlank(fieldName.toString())) {
            return isFill;
        }
        ProtocolDeviceService protocolDeviceService = new ProtocolDeviceService();
        JedisCommands jedis = JedisUtil.getJedis();
        try {
            cacheName = cacheName + fieldType;
            Boolean exists = jedis.exists(cacheName);
            if (exists == null || !exists) {
                LambdaQueryWrapper<ProtocolDevice> queryWrapper = new QueryWrapper<ProtocolDevice>().lambda();
                queryWrapper.ge(ProtocolDevice::getType, fieldType);
                List<ProtocolDevice> protocolDevices = protocolDeviceService.list(queryWrapper);

                for (ProtocolDevice protocolDevice : protocolDevices) {
                    String value = protocolDevice.getPrimaryNamecn() + DpiConstant.DPI_ENCRY_SPECIAL_CHARACTERS
                            + protocolDevice.getSecondaryNamecn() + DpiConstant.DPI_ENCRY_SPECIAL_CHARACTERS
                            + protocolDevice.getVendor() + DpiConstant.DPI_ENCRY_SPECIAL_CHARACTERS
                            + protocolDevice.getServiceDesc() + DpiConstant.DPI_ENCRY_SPECIAL_CHARACTERS
                            + protocolDevice.getCloudPlatform();
                    jedis.hset(cacheName, protocolDevice.getService(), value);
                }
                jedis.expire(cacheName, redisExpire);
            }
            Map<String, String> cacheMap = jedis.hgetAll(cacheName);
            for (Map.Entry<String, String> entry : cacheMap.entrySet()) {
                String field = entry.getKey().toString();
                String value = entry.getValue().toString();
                if (fieldType == 1) {
                    if (fieldName.toString().equalsIgnoreCase(field)) {
                        String[] values = value.split(DpiConstant.DPI_DECRY_SPECIAL_CHARACTERS);
                        dpiModel.setDeviceinfoPrimaryNamecn(values.length > 0 ? values[0] : "");
                        dpiModel.setDeviceinfoSecondaryNamecn(values.length > 1 ? values[1] : "");
                        dpiModel.setDeviceinfoVendor(values.length > 2 ? values[2] : "");
                        dpiModel.setDeviceinfoServiceDesc(values.length > 3 ? values[3] : "");
                        dpiModel.setDeviceinfoCloudPlatform(values.length > 4 ? values[4] : "");

                        isFill = Boolean.TRUE;
                    }
                } else {
                    if (fieldName.toString().toLowerCase().contains(field.toLowerCase())) {
                        String[] values = value.split(DpiConstant.DPI_DECRY_SPECIAL_CHARACTERS);
                        dpiModel.setDeviceinfoPrimaryNamecn(values.length > 0 ? values[0] : "");
                        dpiModel.setDeviceinfoSecondaryNamecn(values.length > 1 ? values[1] : "");
                        dpiModel.setDeviceinfoVendor(values.length > 2 ? values[2] : "");
                        dpiModel.setDeviceinfoServiceDesc(values.length > 3 ? values[3] : "");
                        dpiModel.setDeviceinfoCloudPlatform(values.length > 4 ? values[4] : "");

                        isFill = Boolean.TRUE;
                    }
                }
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage());
            JedisUtil.close(jedis);
        }

        return isFill;
    }

    public void fillIpInfo(DpiModel dpiModel, IpipnetHandler ipipnetHandler, String ip, Boolean srcOrDstIpFlag) {
        String country = "";
        String province = "";
        String city = "";
        String owner = "";
        String ipOperator = "";
        String ipLongitude = "";
        String ipLatitude = "";
        String timeZoneCity = "";
        String timeZone = "";
        String areaCode = "";
        String globalRoaming = "";
        String internalCode = "";
        String stateCode = "";
        String ipArea = "";
        Map<String, Object> ipInfo = ipipnetHandler.handler(ip);
        if (ipInfo != null) {
            // ip所属国家
            country = ipInfo.get("country").toString();
            // ip所属省份
            province = ipInfo.get("province").toString();
            // ip地级市/省直辖县级行政区
            city = ipInfo.get("city").toString();
            // ip段所有者
            owner = ipInfo.get("owner").toString();
            // ip运营商
            ipOperator = ipInfo.get("ipOperator").toString();
            // ip维度
            ipLongitude = ipInfo.get("ipLongitude").toString();
            // ip经度
            ipLatitude = ipInfo.get("ipLatitude").toString();
            // 时区代表城市
            timeZoneCity = ipInfo.get("timeZoneCity").toString();
            // 时区
            timeZone = ipInfo.get("timeZone").toString();
            // 中国行政区划代码
            areaCode = ipInfo.get("areaCode").toString();
            // 国际区号
            globalRoaming = ipInfo.get("globalRoaming").toString();
            // 国际代码
            internalCode = ipInfo.get("internalCode").toString();
            // 州代码
            stateCode = ipInfo.get("stateCode").toString();
            // ip所属区域
            ipArea = ipInfo.get("ipArea").toString();
        }
        if (srcOrDstIpFlag) {
            // 通过ipipnet文件查询ip的基准信息
            dpiModel.setIpinfoCountry(country);
            dpiModel.setIpinfoProvince(province);
            dpiModel.setIpinfoCity(city);
            dpiModel.setIpinfoOwner(owner);
            dpiModel.setIpinfoTimeZoneCity(timeZoneCity);
            dpiModel.setIpinfoTimeZone(timeZone);
            dpiModel.setIpinfoAreaCode(areaCode);
            dpiModel.setIpinfoGlobalRoaming(globalRoaming);
            dpiModel.setIpinfoInternalCode(internalCode);
            dpiModel.setIpinfoStateCode(stateCode);
            dpiModel.setIpinfoOperator(ipOperator);
            dpiModel.setIpinfoLongitude(ipLongitude);
            dpiModel.setIpinfoLatitude(ipLatitude);
            dpiModel.setIpinfoArea(ipArea);
        } else {
            // 通过ipipnet文件查询ip的基准信息
            dpiModel.setDestipinfoCountry(country);
            dpiModel.setDestipinfoProvince(province);
            dpiModel.setDestipinfoCity(city);
            dpiModel.setDestipinfoOwner(owner);
            dpiModel.setDestipinfoTimeZoneCity(timeZoneCity);
            dpiModel.setDestipinfoTimeZone(timeZone);
            dpiModel.setDestipinfoAreaCode(areaCode);
            dpiModel.setDestipinfoGlobalRoaming(globalRoaming);
            dpiModel.setDestipinfoInternalCode(internalCode);
            dpiModel.setDestipinfoStateCode(stateCode);
            dpiModel.setDestipinfoOperator(ipOperator);
            dpiModel.setDestipinfoLongitude(ipLongitude);
            dpiModel.setDestipinfoLatitude(ipLatitude);
            dpiModel.setDestipinfoArea(ipArea);
        }
    }
}
