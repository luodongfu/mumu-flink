package com.lovecws.mumu.flink.common.model.dpi;

import com.lovecws.mumu.flink.common.annotation.EsField;
import com.lovecws.mumu.flink.common.annotation.TableField;
import com.lovecws.mumu.flink.common.annotation.TablePartition;
import com.lovecws.mumu.flink.common.annotation.TableProperties;
import com.baomidou.mybatisplus.extension.activerecord.Model;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * @Author: lyc
 * @Date: 2019-6-26 14:21
 * @Description: dpi实体类
 */
@Data
@TableProperties(storage = "parquet")
@TablePartition(partition = true, partitionType = "", partitionFields = {"ds"}, databaseType = "hive")
public class DpiModel extends Model<DpiModel> implements Serializable {

    @TableField(name = "id", comment = "消息唯一性")
    private String id;

    @EsField(name = "primaryId")
    @TableField(name = "primary_id", comment = "事件主键")
    public String primaryId;

    @EsField(name = "srcIp")
    @TableField(name = "src_ip", comment = "源IP")
    public String srcIp;

    @EsField(name = "destIp")
    @TableField(name = "dest_ip", comment = "目的IP")
    public String destIp;

    @EsField(name = "srcPort")
    @TableField(name = "src_port", comment = "源IP端口")
    public String srcPort;

    @EsField(name = "destPort")
    @TableField(name = "dest_port", comment = "目的IP端口")
    public String destPort;

    @EsField(name = "service")
    @TableField(name = "service", comment = "通信协议")
    public String service;

    @EsField(name = "upPackNum")
    @TableField(name = "up_pack_num", comment = "上传包数")
    public String upPackNum;

    @EsField(name = "upByteNum")
    @TableField(name = "up_byte_num", comment = "上传字节数")
    public String upByteNum;

    @EsField(name = "downPackNum")
    @TableField(name = "down_pack_num", comment = "下载包数")
    public String downPackNum;

    @EsField(name = "downByteNum")
    @TableField(name = "down_byte_num", comment = "下载字节数")
    public String downByteNum;

    @EsField(name = "createTime", type = "date", format = "yyyy-MM-dd HH:mm:ss")
    @TableField(name = "create_time", type = "timestamp", format = "yyyy-MM-dd HH:mm:ss", comment = "事件发生时间")
    public Date createTime;

    @EsField(name = "srcCorpName")
    @TableField(name = "src_corp_name", comment = "源IP单位名称")
    public String srcCorpName;

    @EsField(name = "srcIndustry")
    @TableField(name = "src_industry", comment = "源IP行业")
    public String srcIndustry;

    @EsField(name = "destCorpName")
    @TableField(name = "dest_corp_name", comment = "目的IP单位名称")
    public String destCorpName;

    @EsField(name = "destIndustry")
    @TableField(name = "dest_industry", comment = "目的IP行业")
    public String destIndustry;

    @EsField(name = "dynamicField")
    @TableField(name = "dynamic_field", comment = "动态字段")
    public String dynamicField;

    @EsField(name = "url")
    @TableField(name = "url", comment = "动态字段-url")
    public String url;

    @EsField(name = "data")
    @TableField(name = "data", comment = "动态字段-data")
    public String data;


    @EsField(name = "deviceinfo.primary_namecn")
    @TableField(name = "deviceinfo_primary_namecn", comment = "服务类型")
    public String deviceinfoPrimaryNamecn;

    @EsField(name = "deviceinfo.secondary_namecn")
    @TableField(name = "deviceinfo_secondary_namecn", comment = "设备类型")
    public String deviceinfoSecondaryNamecn;

    @EsField(name = "deviceinfo.vendor")
    @TableField(name = "deviceinfo_vendor", comment = "设备厂商")
    public String deviceinfoVendor;

    @EsField(name = "deviceinfo.service_desc")
    @TableField(name = "deviceinfo_service_desc", comment = "设备通信协议描述")
    public String deviceinfoServiceDesc;

    @EsField(name = "deviceinfo.cloud_platform")
    @TableField(name = "deviceinfo_cloud_platform", comment = "设备联网平台")
    public String deviceinfoCloudPlatform;

    @EsField(name = "deviceinfo.deviceid")
    @TableField(name = "deviceinfo_deviceid", comment = "设备ID")
    public String deviceinfoDeviceid;

    @EsField(name = "deviceinfo.sn")
    @TableField(name = "deviceinfo_sn", comment = "设备SN")
    public String deviceinfoSn;

    @EsField(name = "deviceinfo.mac")
    @TableField(name = "deviceinfo_mac", comment = "设备MAC地址")
    public String deviceinfoMac;

    @EsField(name = "deviceinfo.md5")
    @TableField(name = "deviceinfo_md5", comment = "设备MD5")
    public String deviceinfoMd5;

    @EsField(name = "deviceinfo.os")
    @TableField(name = "deviceinfo_os", comment = "设备操作系统")
    public String deviceinfoOs;

    @EsField(name = "deviceinfo.softVersion")
    @TableField(name = "deviceinfo_softversion", comment = "设备软件版本")
    public String deviceinfoSoftVersion;

    @EsField(name = "deviceinfo.firmwareVersion")
    @TableField(name = "deviceinfo_firmwareversion", comment = "设备固件版本")
    public String deviceinfoFirmwareVersion;

    @EsField(name = "deviceinfo.ip")
    @TableField(name = "deviceinfo_ip", comment = "设备内网IP")
    public String deviceinfoIp;

    @EsField(name = "deviceinfo.number")
    @TableField(name = "deviceinfo_number", comment = "设备注册码")
    public String deviceinfoNumber;


    @EsField(name = "ipinfo.country")
    @TableField(name = "ipinfo_country", comment = "源IP所属国家")
    public String ipinfoCountry;

    @EsField(name = "ipinfo.province")
    @TableField(name = "ipinfo_province", comment = "源IP所属省")
    public String ipinfoProvince;

    @EsField(name = "ipinfo.city")
    @TableField(name = "ipinfo_city", comment = "源IP所属地市")
    public String ipinfoCity;

    @EsField(name = "ipinfo.owner")
    @TableField(name = "ipinfo_owner", comment = "源IP使用者")
    public String ipinfoOwner;

    @EsField(name = "ipinfo.timeZoneCity")
    @TableField(name = "ipinfo_timezonecity", comment = "源IP时区代表城市")
    public String ipinfoTimeZoneCity;

    @EsField(name = "ipinfo.timeZone")
    @TableField(name = "ipinfo_timezone", comment = "源IP时区")
    public String ipinfoTimeZone;

    @EsField(name = "ipinfo.areaCode")
    @TableField(name = "ipinfo_areacode", comment = "源IP中国行政区划代码")
    public String ipinfoAreaCode;

    @EsField(name = "ipinfo.globalRoaming")
    @TableField(name = "ipinfo_globalroaming", comment = "源IP国际区号")
    public String ipinfoGlobalRoaming;

    @EsField(name = "ipinfo.internalCode")
    @TableField(name = "ipinfo_internalcode", comment = "源IP国家编码")
    public String ipinfoInternalCode;

    @EsField(name = "ipinfo.stateCode")
    @TableField(name = "ipinfo_statecode", comment = "源IP州代码")
    public String ipinfoStateCode;

    @EsField(name = "ipinfo.operator")
    @TableField(name = "ipinfo_operator", comment = "源IP运营商")
    public String ipinfoOperator;

    @EsField(name = "ipinfo.longitude")
    @TableField(name = "ipinfo_longitude", comment = "源IP所属纬度")
    public String ipinfoLongitude;

    @EsField(name = "ipinfo.latitude")
    @TableField(name = "ipinfo_latitude", comment = "源IP所属经度")
    public String ipinfoLatitude;

    @EsField(name = "ipinfo.area")
    @TableField(name = "ipinfo_area", comment = "源IP所属地区")
    public String ipinfoArea;


    @EsField(name = "destIpInfo.country")
    @TableField(name = "destipinfo_country", comment = "目的IP所属国家")
    public String destipinfoCountry;

    @EsField(name = "destIpInfo.province")
    @TableField(name = "destipinfo_province", comment = "目的IP所属省")
    public String destipinfoProvince;

    @EsField(name = "destIpInfo.city")
    @TableField(name = "destipinfo_city", comment = "目的IP所属地市")
    public String destipinfoCity;

    @EsField(name = "destIpInfo.owner")
    @TableField(name = "destipinfo_owner", comment = "目的IP使用者")
    public String destipinfoOwner;

    @EsField(name = "destIpInfo.timeZoneCity")
    @TableField(name = "destipinfo_timezonecity", comment = "目的IP时区代表城市")
    public String destipinfoTimeZoneCity;

    @EsField(name = "destIpInfo.timeZone")
    @TableField(name = "destipinfo_timezone", comment = "目的IP时区")
    public String destipinfoTimeZone;

    @EsField(name = "destIpInfo.areaCode")
    @TableField(name = "destipinfo_areacode", comment = "目的IP中国行政区划代码")
    public String destipinfoAreaCode;

    @EsField(name = "destIpInfo.globalRoaming")
    @TableField(name = "destipinfo_globalroaming", comment = "目的IP国际区号")
    public String destipinfoGlobalRoaming;

    @EsField(name = "destIpInfo.internalCode")
    @TableField(name = "destipinfo_internalcode", comment = "目的IP国家编码")
    public String destipinfoInternalCode;

    @EsField(name = "destIpInfo.stateCode")
    @TableField(name = "destipinfo_statecode", comment = "目的IP州代码")
    public String destipinfoStateCode;

    @EsField(name = "destIpInfo.operator")
    @TableField(name = "destipinfo_operator", comment = "目的IP运营商")
    public String destipinfoOperator;

    @EsField(name = "destIpInfo.longitude")
    @TableField(name = "destipinfo_longitude", comment = "目的IP所属纬度")
    public String destipinfoLongitude;

    @EsField(name = "destIpInfo.latitude")
    @TableField(name = "destipinfo_latitude", comment = "目的IP所属经度")
    public String destipinfoLatitude;

    @EsField(name = "destIpInfo.area")
    @TableField(name = "destipinfo_area", comment = "目的IP所属地区")
    public String destipinfoArea;

    @TableField(name = "ds", comment = "年月日分区字段(yyyy-MM-dd)")
    private String ds;

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
