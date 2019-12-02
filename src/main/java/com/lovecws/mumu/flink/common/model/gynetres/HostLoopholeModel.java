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
 * @description: 主机漏洞模型
 * @author: 甘亮
 * @create: 2018-12-04 15:33
 **/
@Data
@EqualsAndHashCode(callSuper = false)
@TableName("tb_loophole")
public class HostLoopholeModel extends Model<HostLoopholeModel> {

    @TableId(value = "id", type = IdType.AUTO)
    private Integer id;

    @TableField("task_id")
    private int taskId;

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

    @TableField("industry")
    private String industry;

    @TableField("industry_type")
    private String industryType;

    @TableField("vendor")
    private String vendor;

    @TableField("vendor_source")
    private String vendorSource;
}
