package com.lovecws.mumu.flink.streaming.task;

import com.lovecws.mumu.flink.common.annotation.ModelLocation;
import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.config.ConfigProperties;
import com.lovecws.mumu.flink.common.model.atd.AtdEventModel;
import com.lovecws.mumu.flink.common.util.IPUtil;
import com.lovecws.mumu.flink.common.util.MD5Util;
import com.lovecws.mumu.flink.streaming.common.cache.SyslogCorpCache;
import com.lovecws.mumu.flink.streaming.common.cache.SyslogDictCache;
import com.lovecws.mumu.flink.streaming.common.handler.HubeiHandler;
import com.lovecws.mumu.flink.streaming.common.handler.IpipnetHandler;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @program: mumu-flink
 * @description: atd安全事件
 * @author: 甘亮
 * @create: 2019-10-31 15:30
 **/
@Slf4j
@ModularAnnotation(type = "task", name = "atd")
public class AtdStreamingTask extends AbstractStreamingTask {

    private static final List<String> EVENT_TYPE_IDS = Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16");
    //攻击方事件类型（源ip攻击目的ip事件类型）
    //private static final List<String> ATTACK_EVENT_TYPES = Arrays.asList("2", "4", "7", "9", "10", "11", "12", "14");

    //被攻击方事件类型(目的ip攻击源ip事件类型)
    /*
    private static final List<String> ATTACKED_EVENT_TYPES = Arrays.asList("1", "6", "3", "13", "15", "16");
    //被攻击方事件二级分类(目的ip攻击源ip事件类型)
    private static final List<String> ATTACKED_EVENT_SUBCATEGORY = Arrays.asList("特洛伊木马通信", "恶意加密流量", "远控工具Teamviewer", "远程工具Oray", "远控工具ShowMyPC", "远控工具Radmin", "远控工具QQRemote", "Web明文口令泄露","疑似受控主机发现", "远控工具指纹识别", "潜在隐私策略违反");
    */

    //被攻击方事件类型(目的ip攻击源ip事件类型)
    //private static final List<String> ATTACKED_EVENT_TYPES = Arrays.asList("1", "6", "3", "13", "15", "16");

    //目的ip攻击源ip事件类型二级分类
    private static final List<String> DESTIP_ATTACK_SRCIP_EVENT_SUBCATEGORY = Arrays.asList("APT恶意样本发现",
            "已知恶意文件", "变种恶意文件", "未知恶意文件", "可疑恶意文件", "恶意加密流量", "Fast-Flux僵尸网络通信", "Web明文口令泄露",
            "挖矿木马通信", "特洛伊木马通信", "远控工具QQRemote", "远控工具Radmin", "远控工具ShowMyPC", "远控工具Teamviewer", "远程工具Oray",
            "远控工具指纹识别", "潜在隐私策略违反", "疑似受控主机发现", "WebShell", "钓鱼欺诈邮件", "WEB访问欺诈网站", "Web访问恶意主机",
            "DNS查询恶意域名", "外联恶意服务器", "恶意加密指纹", "恶意长会话连接", "Shadowsocks翻墙代理流量", "Tor暗网流量", "VPN代理流量",
            "DNS隐秘隧道", "HTTP隐秘隧道", "ICMP隐秘隧道", "恶意软件流量基因检测", "潜在恶意流量", "异常DNS行为", "APT事件", "弱口令"
    );

    //源ip攻击目的ip事件类型二级分类
    private static final List<String> SRCIP_ATTACK_DESTIP_EVENT_SUBCATEGORY = Arrays.asList("APT恶意样本发现",
            "子类型", "启发式恶意软件检测", "恶意服务器扫描", "Web扫描", "网络扫描", "端口扫描", "地址扫描", "路由跟踪", "Web爬虫探测",
            "Web目录遍历", "SQL注入", "XML注入", "PHP注入", "LDAP注入", "XSS注入", "XXE注入", "Web应用攻击",
            "Web敏感文件访问", "HTTP协议构造异常", "Web溢出攻击", "WebHeader异常", "WebShell发现", "SMB勒索软件", "MS17-010-SMB漏洞攻击(CVE-2017-0148)",
            "尝试系统调用", "Web远程代码执行", "FTP成功执行'SITE EXEC'请求", "Shellcode", "启发式WebShell检测", "信息泄露", "FTP账号暴力破解",
            "SSH账号暴力破解", "密码暴力破解", "分布式密码爆破", "FTP账号暴破成功", "SSH账号暴破成功", "密码爆破成功", "垃圾邮件IP命中黑名单", "可疑SMTP源IP发现",
            "SynFlood攻击", "DoS攻击", "获取用户权限失败", "尝试获取用户权限", "成功获取用户权限", "尝试获取管理员权限", "管理员权限提权成功", "CVE漏洞攻击", "Heartbleed漏洞攻击(CVE-2014-0160)",
            "Linux Glibc漏洞(CVE-2015-7547)", "ROCA漏洞(CVE-2017-15361)", "ShellShock破壳漏洞攻击(CVE-2014-6271)", "Struts2-045漏洞攻击(CVE-2017-5638)", "启发式CVE攻击检测",
            "Heartbleed漏洞攻击成功(CVE-2014-0160)", "启发式漏洞利用攻击检测", "毒液攻击漏洞(CVE-2015-3456)", "可疑流量", "混杂攻击模式", "潜在信息泄露", "疑似正常SQL Select语句", "疑似正常SQL条件语句",
            "异常Content-Type", "尝试默认账号登录");

    private IpipnetHandler ipipnetHandler;
    private HubeiHandler hubeiHandler;

    public AtdStreamingTask(Map<String, Object> configMap) {
        super(configMap);
        ipipnetHandler = new IpipnetHandler();
        hubeiHandler = new HubeiHandler();

        //目的ip攻击源ip事件类型
        Object destipAttackSrcipSubCategory = configMap.get("destipAttackSrcipSubCategory");
        if (destipAttackSrcipSubCategory != null) {
            DESTIP_ATTACK_SRCIP_EVENT_SUBCATEGORY.clear();
            DESTIP_ATTACK_SRCIP_EVENT_SUBCATEGORY.addAll(Arrays.asList(destipAttackSrcipSubCategory.toString().split(",")));
        }
        //源ip攻击目的ip事件类型
        Object srcipAttackDestipSubCategory = configMap.get("srcipAttackDestipSubCategory");
        if (srcipAttackDestipSubCategory != null) {
            SRCIP_ATTACK_DESTIP_EVENT_SUBCATEGORY.clear();
            SRCIP_ATTACK_DESTIP_EVENT_SUBCATEGORY.addAll(Arrays.asList(srcipAttackDestipSubCategory.toString().split(",")));
        }
    }

    @Override
    public Object parseData(Object data) {
        AtdEventModel atdModel = new AtdEventModel();
        try {
            if (data == null || StringUtils.isBlank(data.toString())) return atdModel;
            String line = new String((byte[]) data, StandardCharsets.UTF_8);
            String[] lineFields = line.split("\\^");
            Field[] fields = FieldUtils.getAllFields(AtdEventModel.class);

            for (Field field : fields) {
                field.setAccessible(true);
                ModelLocation modelLocation = field.getAnnotation(ModelLocation.class);
                if (modelLocation == null || modelLocation.index() < 0) {
                    continue;
                }
                int index = modelLocation.index();
                if (index >= lineFields.length) {
                    continue;
                }
                try {
                    Object result = lineFields[index];
                    if (result == null || "".equals(result.toString())) {
                        result = modelLocation.defaultVal();
                    }
                    switch (modelLocation.type().toLowerCase()) {
                        case "int":
                            if (StringUtils.isEmpty(result.toString())) result = "0";
                            result = Integer.parseInt(result.toString());
                            break;
                        case "long":
                            if (StringUtils.isEmpty(result.toString())) result = "0";
                            result = Long.parseLong(result.toString());
                            break;
                        case "float":
                            if (StringUtils.isEmpty(result.toString())) result = "0";
                            result = Float.parseFloat(result.toString());
                            break;
                        case "date":
                            if (StringUtils.isNotEmpty(result.toString())) {
                                result = DateUtils.parseDate(result.toString(), modelLocation.format());
                            } else {
                                result = null;
                            }
                            break;
                    }
                    field.set(atdModel, result);
                } catch (Exception ex) {
                    log.error(ex.getLocalizedMessage() + "[" + line + "]");
                }
            }

            atdModel.setCreateTime(new Date());
            Date timestamp = atdModel.getTimestamp();
            if (timestamp == null) timestamp = new Date();
            atdModel.setDs(com.lovecws.mumu.flink.common.util.DateUtils.formatDate(timestamp, "yyyy-MM-dd"));
            //将整行的值计算md5设置为id
            atdModel.setId(MD5Util.md5(line));
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage() + "[" + data + "]", ex);
        }
        return atdModel;
    }

    @Override
    public Object fillData(Object data) {
        AtdEventModel atdModel = (AtdEventModel) data;
        try {
            SyslogCorpCache syslogCorpCache = new SyslogCorpCache();
            SyslogDictCache syslogDictCache = new SyslogDictCache();

            //源ip
            String srcIp = atdModel.getSrcIp();
            String srcCorpType = null;
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

                //源ip企业信息
                Map<String, Object> srcCorpInfo = syslogCorpCache.getCorpInfo(atdModel.getSrcIpValue());
                Object srcCorpid = srcCorpInfo.get("corpid");
                if (srcCorpid != null && StringUtils.isNotEmpty(srcCorpid.toString())) {
                    atdModel.setSrcCorpId(Long.parseLong(srcCorpid.toString()));
                }
                atdModel.setSrcCorpName(srcCorpInfo.getOrDefault("corpname", "").toString());
                atdModel.setSrcIndustry(srcCorpInfo.getOrDefault("industry", "").toString());
                srcCorpType = srcCorpInfo.getOrDefault("corptype", "").toString();
            }
            //目的ip
            String dstIp = atdModel.getDstIp();
            String dstCorpType = null;
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

                //目的ip企业信息
                Map<String, Object> dstCorpInfo = syslogCorpCache.getCorpInfo(atdModel.getDstIpValue());
                Object distCorpid = dstCorpInfo.get("corpid");
                if (distCorpid != null && StringUtils.isNotEmpty(distCorpid.toString())) {
                    atdModel.setCorpId(Long.parseLong(distCorpid.toString()));
                }
                atdModel.setCorpName(dstCorpInfo.getOrDefault("corpname", "").toString());
                atdModel.setIndustry(dstCorpInfo.getOrDefault("industry", "").toString());
                dstCorpType = dstCorpInfo.getOrDefault("corptype", "").toString();
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
            if (StringUtils.isEmpty(eventTypeId)) eventTypeId = "1";
            String subCategory = atdModel.getCategory();
            // 目的ip攻击源ip事件类型
            if (DESTIP_ATTACK_SRCIP_EVENT_SUBCATEGORY.contains(subCategory)) {
                srcIpAttacked(atdModel, dstCorpType, srcCorpType);
            } else {
                srcIpAttack(atdModel, dstCorpType, srcCorpType);
            }
            atdModel.setUploadProvinceCode(ConfigProperties.getString("streaming.defaults.provice.code"));
            atdModel.setUploadProvinceName(ConfigProperties.getString("streaming.defaults.provice.name"));
            atdModel.setDataSource(ConfigProperties.getString("streaming.defaults.provice.source"));
            // 对湖北做特殊处理
            hubeiHandler.handle4atd(atdModel);
            return atdModel;
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage() + "[" + JSON.toJSONString(atdModel) + "]", ex);
        }
        return atdModel;
    }

    @Override
    public Boolean filterData(Object data) {
        AtdEventModel atdModel = (AtdEventModel) data;

        // 如果源ip企业名称且目的ip企业名称都为空，则不保存数据
        /*if (StringUtils.isBlank(atdModel.getSrcCorpName()) && StringUtils.isBlank(atdModel.getCorpName())) {
            log.debug("filter atd data src_corp_name=" + atdModel.getSrcCorpName() + " corp_name=" + atdModel.getCorpName());
            return false;
        }*/
        //判断event_type_id字段是否存在或者在合理范围[1,2.....16]之内，否则过滤这条数据
        if (StringUtils.isBlank(atdModel.getEventTypeId()) || !EVENT_TYPE_IDS.contains(atdModel.getEventTypeId())) {
            log.debug("filter atd data : event_type_id=" + atdModel.getEventTypeId());
            return false;
        }
        return true;
    }

    //源ip是攻击源ip事件类型
    private void srcIpAttack(AtdEventModel atdModel, String dstCorpType, String srcCorpType) {
        atdModel.setAttackIp(atdModel.getSrcIp());
        atdModel.setAttackPort(atdModel.getSrcPort());
        atdModel.setAttackCountry(atdModel.getSrcIpCountry());
        atdModel.setAttackProvince(atdModel.getSrcIpProvince());
        atdModel.setAttackCity(atdModel.getSrcIpCity());
        atdModel.setAttackCorpName(atdModel.getSrcCorpName());
        atdModel.setAttackIndustry(atdModel.getSrcIndustry());
        atdModel.setAttackCorpType(srcCorpType);

        atdModel.setAttackedIp(atdModel.getDstIp());
        atdModel.setAttackedPort(atdModel.getDstPort());
        atdModel.setAttackedCountry(atdModel.getDstIpCountry());
        atdModel.setAttackedProvince(atdModel.getDstIpProvince());
        atdModel.setAttackedCity(atdModel.getDstIpCity());
        atdModel.setAttackedCorpName(atdModel.getCorpName());
        atdModel.setAttackedIndustry(atdModel.getIndustry());
        atdModel.setAttackedCorpType(dstCorpType);
    }

    //目的ip是攻击源ip事件类型
    private void srcIpAttacked(AtdEventModel atdModel, String dstCorpType, String srcCorpType) {
        atdModel.setAttackIp(atdModel.getDstIp());
        atdModel.setAttackPort(atdModel.getDstPort());
        atdModel.setAttackCountry(atdModel.getDstIpCountry());
        atdModel.setAttackProvince(atdModel.getDstIpProvince());
        atdModel.setAttackCity(atdModel.getDstIpCity());
        atdModel.setAttackCorpName(atdModel.getCorpName());
        atdModel.setAttackIndustry(atdModel.getIndustry());
        atdModel.setAttackGeoloc(atdModel.getDstIpGeoloc());
        atdModel.setAttackCorpType(dstCorpType);

        atdModel.setAttackedIp(atdModel.getSrcIp());
        atdModel.setAttackedPort(atdModel.getSrcPort());
        atdModel.setAttackedCountry(atdModel.getSrcIpCountry());
        atdModel.setAttackedProvince(atdModel.getSrcIpProvince());
        atdModel.setAttackedCity(atdModel.getSrcIpCity());
        atdModel.setAttackedCorpName(atdModel.getSrcCorpName());
        atdModel.setAttackedIndustry(atdModel.getSrcIndustry());
        atdModel.setAttackedGeoloc(atdModel.getSrcIpGeoloc());
        atdModel.setAttackedCorpType(srcCorpType);
    }
}
