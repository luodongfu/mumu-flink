package com.lovecws.mumu.flink.common.model.gynetres;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.extension.activerecord.Model;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;

/**
 * @program: act-able
 * @description: 部系统漏洞模型
 * @author: wucw
 * @create: 2019-7-11 20:29
 **/
@Data
@EqualsAndHashCode(callSuper = false)
@TableName("tb_loophole_result")
public class MinistryLoopholeResultModel extends Model<MinistryLoopholeResultModel> {

    @TableId(value = "id", type = IdType.AUTO)
    private Integer id;

    @TableField("task_id")
    private Integer taskId;

    @TableField("task_instance_id")
    private String taskInstanceId;

    @TableField("ip")
    private String ip;

    @TableField("port")
    private String port;

    @TableField("ip_operator")
    private String ipOperator;

    @TableField("device_type")
    private String deviceType;

    @TableField("loophole_type")
    private String loopholeType;

    @TableField("ip_area")
    private String ipArea;

    @TableField("device_name")
    private String deviceName;

    @TableField("damage_level")
    private String damageLevel;

    @TableField("data_source")
    private String dataSource;

    @TableField("protocol")
    private String protocol;

    @TableField("protocol_type")
    private String protocolType;

    @TableField("threat")
    private String threat;

    @TableField("device_version")
    private String deviceVersion;

    @TableField("cnvd_no")
    private String cnvdNo;

    @TableField("loophole_description")
    private String loopholeDescription;

    @TableField("sf_version")
    private String sfVersion;

    @TableField("protocol_version")
    private String protocolVersion;

    @TableField("event_id")
    private String eventId;

    @TableField("create_time")
    private Date createTime;

    @TableField("service_type")
    private String serviceType;
    
    @TableField("service_version")
    private String serviceVersion;

    @TableField("service_type_cn")
    private String serviceTypeCn;

    @TableField("event_source")
    private int eventSource;

    @TableField("ip_areacode")
    private String ipAreacode;

    @TableField("ip_globalroaming")
    private String ipGlobalroaming;

    @TableField("ip_useunit")
    private String ipUseunit;
    
    @TableField("ip_useunit_nature")
    private String ipUseunitNature;

    @TableField("industry")
    private String industry;

    @TableField("industry_type")
    private String industryType;

    @TableField("loophole_name")
    private String loopholeName;

    @TableField("loophole_validate")
    private String loopholeValidate;
    
    @TableField("cve_no")
    private String cveNo;

    @TableField("cnnvd_no")
    private String cnnvdNo;
    
    @TableField("upload_province_code")
    private String uploadProvinceCode;
    
    @TableField("upload_province_name")
    private String uploadProvinceName;
    
    @TableField("uuid")
    private String uuid;
    
    @TableField("country")
    private String country;
    
    @TableField("province")
    private String province;
    
    @TableField("city")
    private String city;
    
    @TableField("find_time")
    private Date findTime;
    
    // 操作系统
    @TableField("os")
    private String os;
    
    // 设备厂商 （西门子  卡西欧）
    @TableField("device_vendor")
    private String deviceVendor;
    
    // 
    @TableField("loophole_cpe")
    private String loopholeCpe;
    
    // 是否工控漏洞
    @TableField("is_ics")
    private Boolean isIcs;
    
    // 漏洞修复方案
    @TableField("loophole_advisory")
    private String loopholeAdvisory;
    
    // 参考网址
    @TableField("loophole_ref_website")
    private String loopholeRefWebsite;

    // 云平台名称
    @TableField("cloud_platform_name")
    private String cloudPlatformName;

    // 入库时间
    @TableField("insert_time")
    private Date insertTime;
}
