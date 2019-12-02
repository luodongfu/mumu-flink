package com.lovecws.mumu.flink.common.model.atd;

import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.baomidou.mybatisplus.extension.activerecord.Model;
import com.lovecws.mumu.flink.common.annotation.*;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * atd安全事件模型
 */
@Data
@TableProperties(storage = "parquet")
@TablePartition(partition = true, partitionType = "", partitionFields = {"event_type_id", "ds"}, databaseType = "hive")
public class AtdEventModel extends Model<AtdEventModel> implements Serializable {

    @TableField(name = "id", comment = "消息唯一性")
    private String id;

    @ModelLocation(index = 0, type = "long", defaultVal = "101")
    @EsField(name = "waId")
    @TableField(name = "wa_id", type = "bigint", comment = "网安业务id")
    private Long waId;

    //目的ip企业id
    @EsField(name = "corpId", type = "long")
    @TableField(name = "corp_id", type = "bigint", comment = "目的ip企业id")
    private Long corpId;

    //目的ip企业名称
    @EsField(name = "corpName")
    @TableField(name = "corp_name", comment = "目的ip企业名称")
    private String corpName;

    //目的ip企业行业分类
    @EsField(name = "industry")
    @TableField(name = "industry", comment = "目的ip行业分类")
    private String industry;

    //源ip企业id
    @EsField(name = "srcCorpId")
    @TableField(name = "src_corp_id", type = "bigint", comment = "源ip企业id")
    private Long srcCorpId;

    //源ip企业名称
    @EsField(name = "srcCorpName")
    @TableField(name = "src_corp_name", comment = "源ip企业名称")
    private String srcCorpName;

    //源ip企业行业
    @EsField(name = "srcIndustry")
    @TableField(name = "src_industry", comment = "源ip行业分类")
    private String srcIndustry;

    @JSONField(serialzeFeatures = SerializerFeature.WriteDateUseDateFormat, format = "yyyy-MM-dd HH:mm:ss")
    @EsField(name = "beginTime", type = "date", format = "yyyy-MM-dd HH:mm:ss")
    private Date beginTime;

    @JSONField(serialzeFeatures = SerializerFeature.WriteDateUseDateFormat, format = "yyyy-MM-dd HH:mm:ss")
    @EsField(name = "stopTime", type = "date", format = "yyyy-MM-dd HH:mm:ss")
    private Date stopTime;

    @EsField(name = "srcIpValue",type = "long")
    @TableField(name = "src_ip_value", type = "bigint", comment = "源ip值")
    private Long srcIpValue;

    @EsField(name = "dstIpValue",type = "long")
    @TableField(name = "dst_ip_value", type = "bigint", comment = "目的ip值")
    private Long dstIpValue;


    @ModelLocation(index = 2, type = "date", format = "yyyy-MM-dd HH:mm:ss")
    @EsField(name = "timestamp", type = "date", format = "yyyy-MM-dd HH:mm:ss")
    @TableField(name = "event_time", type = "timestamp", format = "yyyy-MM-dd HH:mm:ss", comment = "事件发生时间")
    private Date timestamp;

    @ModelLocation(index = 3)
    @EsField(name = "sensorName")
    @TableField(name = "sensor_name", comment = "设备主机名")
    private String sensorName;

    @ModelLocation(index = 4)
    @EsField(name = "sensorIp")
    @TableField(name = "sensor_ip", comment = "检测设备主机IP")
    private String sensorIp;

    @ModelLocation(index = 5)
    @EsField(name = "eventSource")
    @TableField(name = "event_source", comment = "数据来源")
    private String eventSource;

    @ModelLocation(index = 6)
    @EsField(name = "uuid")
    @TableField(name = "uuid", comment = "事件uuid")
    private String uuid;

    @EsField(name = "streamMonitorInterface")
    @TableField(name = "stream_monitor_interface", comment = "流量监测网卡")
    private String streamMonitorInterface;

    @ModelLocation(index = 7)
    @EsField(name = "rid")
    @TableField(name = "rid", comment = "事件规则id")
    private String rid;

    @ModelLocation(index = 8)
    @EsField(name = "classtype")
    @TableField(name = "classtype", comment = "威胁大类")
    private String classtype;

    @ModelLocation(index = 8)
    @EsField(name = "classtypeCn")
//    @TableField(name = "classtype_cn")
    private String classtypeCn;

    @ModelLocation(index = 9)
    @EsField(name = "category")
    @TableField(name = "category", comment = "威胁类别")
    private String category;

    @ModelLocation(index = 9)
    @EsField(name = "categoryCn")
//    @TableField(name = "category_cn")
    private String categoryCn;

    @ModelLocation(index = 10)
    @EsField(name = "subCategory")
    @TableField(name = "sub_category", comment = "威胁子类别")
    private String subCategory;

    @ModelLocation(index = 11)
    @EsField(name = "srcIp")
    @TableField(name = "src_ip", comment = "源ip")
    private String srcIp;

    @ModelLocation(index = 12)
    @EsField(name = "srcPort")
    @TableField(name = "src_port", comment = "源端口")
    private String srcPort;

    @ModelLocation(index = 13)
    @EsField(name = "srcService")
    @TableField(name = "src_service", comment = "源ip服务")
    private String srcService;

    @ModelLocation(index = 14)
    @EsField(name = "dstIp")
    @TableField(name = "dst_ip", comment = "目的ip")
    private String dstIp;

    @ModelLocation(index = 15)
    @EsField(name = "dstPort")
    @TableField(name = "dst_port", comment = "目的端口")
    private String dstPort;

    @ModelLocation(index = 16)
    @EsField(name = "dstService")
    @TableField(name = "dst_service", comment = "目的服务")
    private String dstService;

    @ModelLocation(index = 17)
    @EsField(name = "proto")
    @TableField(name = "proto", comment = "传输层协议")
    private String proto;

    @ModelLocation(index = 18)
    @EsField(name = "appProto")
    @TableField(name = "app_proto", comment = "应用层协议")
    private String appProto;

    @ModelLocation(index = 19)
    @EsField(name = "killChain")
    @TableField(name = "kill_chain", comment = "事件攻击链位置")
    private String killChain;

    @ModelLocation(index = 19)
    @EsField(name = "killChainCn")
//    @TableField(name = "kill_chain_cn")
    private String killChainCn;

    @ModelLocation(index = 20)
    @EsField(name = "payload")
    @TableField(name = "payload", comment = "攻击/异常载荷")
    private String payload;

    @ModelLocation(index = 21)
    @EsField(name = "respData")
    @TableField(name = "resp_data", comment = "攻击返回数据")
    private String respData;

    @ModelLocation(index = 22,type = "int")
    @EsField(name = "severity")
    @TableField(name = "severity",type = "int", comment = "威胁严重性")
    private int severity;

    @EsField(name = "severityCn")
    @TableField(name = "severity_cn", comment = "威胁严重性中文")
    private String severityCn;


    @ModelLocation(index = 23)
    @EsField(name = "reliability")
    @TableField(name = "reliability", comment = "事件可信度")
    private String reliability;

    @ModelLocation(index = 24)
    @EsField(name = "family")
    @TableField(name = "family", comment = "威胁所在家族")
    private String family;

    @ModelLocation(index = 25)
    @EsField(name = "target")
    @TableField(name = "target", comment = "威胁对象")
    private String target;

    @ModelLocation(index = 26)
    @EsField(name = "desc")
    @TableField(name = "atd_desc", comment = "威胁描述")
    private String atdDesc;

    @ModelLocation(index = 27)
    @EsField(name = "message")
    @TableField(name = "message", comment = "详细信息")
    private String message;

    @ModelLocation(index = 28)
    @EsField(name = "srcIpCountry")
    @TableField(name = "src_ip_country", comment = "源IP所在国家名")
    private String srcIpCountry;

    @ModelLocation(index = 29)
    @EsField(name = "srcIpProvince")
    @TableField(name = "src_ip_province", comment = "源IP所在省份")
    private String srcIpProvince;

    @ModelLocation(index = 30)
    @EsField(name = "srcIpCity")
    @TableField(name = "src_ip_city", comment = "源IP所在城市")
    private String srcIpCity;

    @ModelLocation(index = 31)
    @EsField(name = "srcIpCounty")
    @TableField(name = "src_ip_county", comment = "源IP所在区")
    private String srcIpCounty;

    @ModelLocation(index = 32)
    @EsField(name = "srcIspName")
    @TableField(name = "src_isp_name", comment = "源IP运营商")
    private String srcIspName;

    @ModelLocation(index = 33)
    @EsField(name = "srcIpGeoloc")
    @TableField(name = "src_ip_geoloc", comment = "源IP经纬度")
    private String srcIpGeoloc;

    @ModelLocation(index = 34)
    @EsField(name = "srcIpCountryCode")
    @TableField(name = "src_ip_country_code", comment = "源IP国家代码")
    private String srcIpCountryCode;

    @ModelLocation(index = 35)
    @EsField(name = "dstIpCountry")
    @TableField(name = "dst_ip_country", comment = "目的IP所在国家名")
    private String dstIpCountry;

    @ModelLocation(index = 36)
    @EsField(name = "dstIpProvince")
    @TableField(name = "dst_ip_province", comment = "目的IP所在省份")
    private String dstIpProvince;

    @ModelLocation(index = 37)
    @EsField(name = "dstIpCity")
    @TableField(name = "dst_ip_city", comment = "目的IP所在城市")
    private String dstIpCity;

    @ModelLocation(index = 38)
    @EsField(name = "dstIpCounty")
    @TableField(name = "dst_ip_county", comment = "目的IP所在地区")
    private String dstIpCounty;

    @ModelLocation(index = 39)
    @EsField(name = "dstIspName")
    @TableField(name = "dst_isp_name", comment = "目的IP运营商")
    private String dstIspName;

    @ModelLocation(index = 40)
    @EsField(name = "dstIpGeoloc")
    @TableField(name = "dst_ip_geoloc", comment = "目的IP经纬度")
    private String dstIpGeoloc;

    @ModelLocation(index = 41)
    @EsField(name = "dstIpCountryCode")
    @TableField(name = "dst_ip_country_code", comment = "目的IP国家代码")
    private String dstIpCountryCode;

    @ModelLocation(index = 42)
    @EsField(name = "conn.srcMac")
    @TableField(name = "conn_src_mac", comment = "源主机Mac地址")
    private String connSrcMac;

    @ModelLocation(index = 43)
    @EsField(name = "conn.dstMac")
    @TableField(name = "conn_dst_mac", comment = "目的主机Mac地址")
    private String connDstMac;

    @ModelLocation(index = 44)
    @EsField(name = "conn.origBytes")
    @TableField(name = "conn_orig_bytes", comment = "源发送字节大小")
    private String connOrigBytes;

    @ModelLocation(index = 45)
    @EsField(name = "conn.respBytes")
    @TableField(name = "conn_resp_bytes", comment = "目的发送字节大小")
    private String connRespBytes;

    @ModelLocation(index = 16)
    @EsField(name = "conn.origPkts")
    @TableField(name = "conn_orig_pkts", comment = "源发送包数量")
    private String connOrigPkts;

    @ModelLocation(index = 47)
    @EsField(name = "conn.respPkts")
    @TableField(name = "conn_resp_pkts", comment = "目的发送包数量")
    private String connRespPkts;

    @ModelLocation(index = 48)
    @EsField(name = "conn.connState")
    @TableField(name = "conn_conn_state", comment = "会话状态")
    private String connConnState;

    @ModelLocation(index = 49)
    @EsField(name = "conn.duration")
    @TableField(name = "conn_duration", comment = "持续时间")
    private String connDuration;

    @ModelLocation(index = 50)
    @EsField(name = "http.method")
    @TableField(name = "http_method", comment = "HTTP方法")
    private String httpMethod;

    @ModelLocation(index = 51)
    @EsField(name = "http.host")
    @TableField(name = "http_host", comment = "主机名")
    private String httpHost;

    @ModelLocation(index = 52)
    @EsField(name = "http.uri")
    @TableField(name = "http_uri", comment = "访问URI")
    private String httpUri;

    @ModelLocation(index = 53)
    @EsField(name = "http.referrer")
    @TableField(name = "http_referrer", comment = "跳转前地址")
    private String httpReferrer;

    @ModelLocation(index = 54)
    @EsField(name = "http.version")
    @TableField(name = "http_version", comment = "HTTP协议版本")
    private String httpVersion;

    @ModelLocation(index = 55)
    @EsField(name = "http.userAgent")
    @TableField(name = "http_user_agent", comment = "浏览器UA字段")
    private String httpUserAgent;

    @ModelLocation(index = 56)
    @EsField(name = "http.contentType")
    @TableField(name = "http_content_type", comment = "返回内容类型")
    private String httpContentType;

    @ModelLocation(index = 57)
    @EsField(name = "http.contentLength")
    @TableField(name = "http_content_length", comment = "返回内容长度")
    private String httpContentLength;

    @ModelLocation(index = 58)
    @EsField(name = "http.statusCode")
    @TableField(name = "http_status_code", comment = "返回码")
    private String httpStatusCode;

    @ModelLocation(index = 59)
    @EsField(name = "dns.query")
    @TableField(name = "dns_query", comment = "查询内容")
    private String dnsQuery;

    @ModelLocation(index = 60)
    @EsField(name = "dns.qtypeName")
    @TableField(name = "dns_qtype_name", comment = "查询类型")
    private String dnsQtypeName;

    @ModelLocation(index = 61)
    @EsField(name = "dns.ttl")
    @TableField(name = "dns_ttl", comment = "生存周期")
    private String dnsTtl;

    @ModelLocation(index = 62)
    @EsField(name = "dns.answers")
    @TableField(name = "dns_answers", comment = "返回值")
    private String dnsAnswers;

    @ModelLocation(index = 63)
    @EsField(name = "sumstat.number")
    @TableField(name = "sumstat_number", comment = "发现次数")
    private String sumstatNumber;

    @ModelLocation(index = 64)
    @EsField(name = "sumstat.duration")
    @TableField(name = "sumstat_duration", comment = "统计周期")
    private String sumstatDuration;

    @ModelLocation(index = 65)
    @EsField(name = "sumstat.sample")
    @TableField(name = "sumstat_sample", comment = "威胁数据取样")
    private String sumstatSample;

    @EsField(name = "standby1")
    @TableField(name = "standby1", comment = "预留字段")
    private String standby1;

    @EsField(name = "standby2")
    @TableField(name = "standby2", comment = "预留字段")
    private String standby2;

    @EsField(name = "standby3")
    @TableField(name = "standby3", comment = "预留字段")
    private String standby3;

    @EsField(name = "standby4")
    @TableField(name = "standby4", comment = "预留字段")
    private String standby4;

    @ModelLocation(index = 66)
    @EsField(name = "standby5")
    @TableField(name = "standby5", comment = "预留字段")
    private String standby5;

    @EsField(name = "standby6")
    @ModelLocation(index = 67)
    @TableField(name = "standby6", comment = "预留字段")
    private String standby6;

    @EsField(name = "standby7")
    @ModelLocation(index = 68)
    @TableField(name = "standby7", comment = "预留字段")
    private String standby7;

    @ModelLocation(index = 69)
    @EsField(name = "standby8")
    @TableField(name = "standby8", comment = "预留字段")
    private String standby8;

    @ModelLocation(index = 70)
    @EsField(name = "standby9")
    @TableField(name = "standby9", comment = "预留字段")
    private String standby9;

    @ModelLocation(index = 71)
    @EsField(name = "ftp.cmd")
    @TableField(name = "ftp_cmd", comment = "ftp命令")
    private String ftpFtpCmd;

    @ModelLocation(index = 72)
    @EsField(name = "smtp.from")
    @TableField(name = "smtp_from", comment = "SMTP发送人")
    private String smtpSmtpFrom;

    @ModelLocation(index = 73)
    @EsField(name = "smtp.to")
    @TableField(name = "smtp_to", comment = "SMTP收件人")
    private String smtpSmtpTo;

    @ModelLocation(index = 74)
    @EsField(name = "standby13")
    @TableField(name = "standby13", comment = "预留字段")
    private String standby13;

    @ModelLocation(index = 75)
    @EsField(name = "standby14")
    @TableField(name = "standby14", comment = "预留字段")
    private String standby14;

    @ModelLocation(index = 76)
    @EsField(name = "file.package")
    @TableField(name = "file_package", comment = "分析包")
    private String filePackage;

    @ModelLocation(index = 77)
    @EsField(name = "file.sha256")
    @TableField(name = "file_sha256", comment = "文件SHA256值")
    private String fileSha256;

    @ModelLocation(index = 78)
    @EsField(name = "file.ssdeep")
    @TableField(name = "file_ssdeep", comment = "相关文件SSDEEP值")
    private String fileSsdeep;

    @ModelLocation(index = 79)
    @EsField(name = "file.filename")
    @TableField(name = "file_filename", comment = "文件真实名称")
    private String fileFilename;

    @ModelLocation(index = 80)
    @EsField(name = "file.tag")
    @TableField(name = "file_tag", comment = "文件tag标记")
    private String fileTag;

    @ModelLocation(index = 81)
    @EsField(name = "file.md5")
    @TableField(name = "file_md5", comment = "相关文件MD5值")
    private String fileMd5;

    @ModelLocation(index = 82)
    @EsField(name = "standby15")
    @TableField(name = "standby15", comment = "standby15")
    private String standby15;

    @ModelLocation(index = 83)
    @EsField(name = "standby16")
    private String standby16;

    @EsField(name = "standby17")
    private String standby17;

    @EsField(name = "standby18")
    private String standby18;

    @ModelLocation(index = 84)
    @EsField(name = "standby19")
    private String standby19;

    @ModelLocation(index = 85)
    @EsField(name = "standby20")
    private String standby20;

    @ModelLocation(index = 88, type = "date", format = "yyyy-MM-dd HH:mm:ss")
    @EsField(name = "waCreateTime", type = "date", format = "yyyy-MM-dd HH:mm:ss")
    @TableField(name = "wa_create_time", type = "timestamp", comment = "网安事件处理时间")
    private Date waCreateTime;

    @EsField(name = "createTime", type = "date", format = "yyyy-MM-dd HH:mm:ss")
    @TableField(name = "create_time", type = "timestamp", comment = "事件处理时间")
    private Date createTime;

    @EsField(name = "count", type = "long")
    private Long count;

    @EsField(name = "beginTimeStr")
    private String beginTimeStr;

    @EsField(name = "stopTimeStr")
    private String stopTimeStr;

    @EsField(name = "srcIpCountryFromIpNet")
    @TableField(name = "src_ip_country_from_ipnet", comment = "从ipipnet获取的源ip国家名称")
    private String srcIpCountryFromIpNet;

    @EsField(name = "srcIpCountryCodeFromIpNet")
    @TableField(name = "src_ip_country_code_from_ipnet", comment = "从ipipnet获取的源ip国家编码")
    private String srcIpCountryCodeFromIpNet;

    @EsField(name = "srcIpProvinceFromIpNet")
    @TableField(name = "src_ip_province_from_ipnet", comment = "从ipipnet获取的源ip省份")
    private String srcIpProvinceFromIpNet;

    @EsField(name = "srcIpProvinceCodeFromIpNet")
    @TableField(name = "src_ip_province_code_from_ipnet", comment = "从ipipnet获取的源ip省份编码")
    private String srcIpProvinceCodeFromIpNet;

    @EsField(name = "srcIpCityFromIpNet")
    @TableField(name = "src_ip_city_from_ipnet", comment = "从ipipnet获取的源ip城市")
    private String srcIpCityFromIpNet;

    @EsField(name = "srcIpCityCodeFromIpNet")
    @TableField(name = "src_ip_city_code_from_ipnet", comment = "从ipipnet获取的源ip城市编码")
    private String srcIpCityCodeFromIpNet;

    @EsField(name = "dstIpCountryFromIpNet")
    @TableField(name = "dst_ip_country_from_ipnet", comment = "从ipipnet获取的目的ip国家名称")
    private String dstIpCountryFromIpNet;

    @EsField(name = "dstIpCountryCodeFromIpNet")
    @TableField(name = "dst_ip_country_code_from_ipnet", comment = "从ipipnet获取的目的ip国家编码")
    private String dstIpCountryCodeFromIpNet;

    //查询ipnet-目的ip省份
    @EsField(name = "dstIpProvinceFromIpNet")
    @TableField(name = "dst_ip_province_from_ipnet", comment = "从ipipnet获取的目的ip省份")
    private String dstIpProvinceFromIpNet;

    @EsField(name = "dstIpProvinceCodeFromIpNet")
    @TableField(name = "dst_ip_province_code_from_ipnet", comment = "从ipipnet获取的目的ip省份编码")
    private String dstIpProvinceCodeFromIpNet;

    //查询ipnet-目的ip城市
    @EsField(name = "dstIpCityFromIpNet")
    @TableField(name = "dst_ip_city_from_ipnet", comment = "从ipipnet获取的目的ip城市")
    private String dstIpCityFromIpNet;

    //查询ipnet-目的ip地市编码
    @EsField(name = "dstIpCityCodeFromIpNet")
    @TableField(name = "dst_ip_city_code_from_ipnet", comment = "从ipipnet获取的目的ip城市编码")
    private String dstIpCityCodeFromIpNet;

    //查询ipnet-源ip类型(ipv4/ipv6)
    @EsField(name = "srcIpType")
    @TableField(name = "src_ip_type", comment = "源ip类型(ipv4,ipv6)")
    private String srcIpType;

    //查询ipnet-目的ip类型(ipv4/ipv6)
    @EsField(name = "dstIpType")
    @TableField(name = "dst_ip_type", comment = "目的ip类型(ipv4,ipv6)")
    private String dstIpType;

    /**
     * 113.247.226.130_15137_59.51.78.210_53
     */
    @EsField(name = "ipPortJoint")
    private String ipPortJoint;

    @EsField(name = "srcIpPort")
    private String srcIpPort;

    //119.39.48.120_223.5.5.5
    @EsField(name = "ipJoint")
    private String ipJoint;

    @EsField(name = "dstIpPort")
    private String dstIpPort;

    //攻击方
    @TableField(name = "attack_ip", comment = "攻击方ip")
    @EsField(name = "attack.ip")
    private String attackIp;

    @TableField(name = "attack_port", comment = "攻击方端口")
    @EsField(name = "attack.port")
    private String attackPort;

    @TableField(name = "attack_country", comment = "攻击方国家")
    @EsField(name = "attack.country")
    private String attackCountry;

    @TableField(name = "attack_province", comment = "攻击方省份")
    @EsField(name = "attack.province")
    private String attackProvince;

    @TableField(name = "attack_city", comment = "攻击方城市")
    @EsField(name = "attack.city")
    private String attackCity;

    @TableField(name = "attack_corpname", comment = "攻击方企业名称")
    @EsField(name = "attack.corpname")
    private String attackCorpName;

    @TableField(name = "attack_industry", comment = "攻击方行业分类")
    @EsField(name = "attack.industry")
    private String attackIndustry;

    @TableField(name = "attack_geoloc", comment = "攻击方经纬度")
    @EsField(name = "attack.geoloc")
    private String attackGeoloc;

    //被攻击方
    @TableField(name = "attacked_ip", comment = "被攻击方ip")
    @EsField(name = "attacked.ip")
    private String attackedIp;

    @TableField(name = "attacked_port", comment = "被攻击方端口")
    @EsField(name = "attacked.port")
    private String attackedPort;

    @TableField(name = "attacked_country", comment = "被攻击方国家")
    @EsField(name = "attacked.country")
    private String attackedCountry;

    @TableField(name = "attacked_province", comment = "被攻击方省份")
    @EsField(name = "attacked.province")
    private String attackedProvince;

    @TableField(name = "attacked_city", comment = "被攻击方城市名称")
    @EsField(name = "attacked.city")
    private String attackedCity;

    @TableField(name = "attacked_corpname", comment = "被攻击方企业名称")
    @EsField(name = "attacked.corpname")
    private String attackedCorpName;

    @TableField(name = "attacked_industry", comment = "被攻击方行业分类")
    @EsField(name = "attacked.industry")
    private String attackedIndustry;

    @TableField(name = "attacked_geoloc", comment = "被攻击方的经纬度")
    @EsField(name = "attacked.geoloc")
    private String attackedGeoloc;

    /**
     * 分区字段
     */
    @ModelLocation(index = 1)
    @EsField(name = "eventTypeId")
    @TableField(name = "event_type_id", comment = "事件类型分区字段")
    private String eventTypeId;

    /**
     * 分区字段
     */
    @TableField(name = "ds", comment = "年月日分区字段(yyyy-MM-dd)")
    private String ds;
    
    @TableField(name = "attack_corp_type", comment = "攻击方企业性质")
    @EsField(name = "attack.corpType")
    private String attackCorpType;
    
    @TableField(name = "attacked_corp_type", comment = "被攻击方企业性质")
    @EsField(name = "attacked.corpType")
    private String attackedCorpType;
    /**
     * uploadProvinceCode：上报网省编码
     * uploadProvinceName：上报网省名称
     * dataSource：数据来源：0：亚鸿 1：第三方
     * mapper.atd、dpi均要加上这三个字段，dataSource均为1
     */
    @TableField(name = "upload_province_code", comment = "上报网省编码")
    @EsField(name = "uploadProvinceCode")
    private String uploadProvinceCode;
    
    @TableField(name = "upload_province_name", comment = "上报网省名称")
    @EsField(name = "uploadProvinceName")
    private String uploadProvinceName;
    
    @TableField(name = "data_source", comment = "数据来源")
    @EsField(name = "dataSource")
    private String dataSource;
    
}
