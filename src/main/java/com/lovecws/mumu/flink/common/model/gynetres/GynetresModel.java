package com.lovecws.mumu.flink.common.model.gynetres;

import com.baomidou.mybatisplus.extension.activerecord.Model;
import com.lovecws.mumu.flink.common.annotation.*;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * @program: act-able
 * @description: 资产探测数据模型
 * AvroField字段注解 标注从kafka数据源avro字段
 * EsField 字段注解 标注这个字段需要保存到es
 * TableField 字段注解 标注这个字段需要保存到hive
 * @author: 甘亮
 * @create: 2019-06-05 18:03
 **/
@Data
@TableProperties(storage = "parquet")
@TablePartition(partition = true, partitionFields = {"ds"}, databaseType = "hive")
public class GynetresModel extends Model<GynetresModel> implements Serializable {

    //主键 接收消息唯一建
    @EsField(name = "id")
    @TableField(name = "id", comment = "id")
    private String id;

    @AvroField(name = "primaryTypeName")
    @EsField(name = "primary_type.name")
    @TableField(name = "primarytype_name", comment = "设备一级分类")
    private String primaryTypeName;

    @AvroField(name = "primaryTypeNamecn")
    @EsField(name = "primary_type.namecn")
    @TableField(name = "primarytype_namecn", comment = "设备一级分类中文")
    private String primaryTypeNamecn;

    @AvroField(name = "scannerLevel")
    @EsField(name = "scanner_level")
    @TableField(name = "scanner_level", comment = "扫描等级")
    private String scannerLevel;

    @AvroField(name = "certificate")
    @EsField(name = "certificate")
    @TableField(name = "certificate", comment = "认证信息")
    private String certificate;

    @AvroField(name = "taskInstanceId")
    @EsField(name = "task_instance_id")
    @TableField(name = "task_instance_id", comment = "扫描程序任务实例id")
    private String taskInstanceId;

    @AvroField(name = "description")
    @EsField(name = "description")
    @TableField(name = "description", comment = "描述信息")
    private String description;

    @AvroField(name = "moduleNumber")
    @EsField(name = "module_number")
    @TableField(name = "module_number", comment = "型号数字编码")
    private String moduleNumber;

    @AvroField(name = "scanType")
    @EsField(name = "scan_type")
    @TableField(name = "scan_type", comment = "扫描类型")
    private String scanType;

    @AvroField(name = "taskId")
    @EsField(name = "task_id")
    @TableField(name = "task_id", comment = "扫描程序任务id")
    private String taskId;

    @AvroField(name = "serviceVersion")
    @EsField(name = "service_version")
    @TableField(name = "service_version", comment = "服务版本号")
    private String serviceVersion;

    @AvroField(name = "deviceName")
    @EsField(name = "device_name")
    @TableField(name = "device_name", comment = "设备名称")
    private String deviceName;

    @AvroField(name = "protocol")
    @EsField(name = "protocol")
    @TableField(name = "protocol", comment = "协议类型")
    private String protocol;

    @AvroField(name = "moduleNum")
    @EsField(name = "module_num")
    @TableField(name = "module_num", comment = "型号版本号")
    private String moduleNum;

    @AvroField(name = "vendor")
    @EsField(name = "vendor")
    @TableField(name = "vendor", comment = "厂商")
    private String vendor;

    @AvroField(name = "vendorSource")
    @EsField(name = "vendor_source")
    @TableField(name = "vendor_source", comment = "厂商来源(国内，境外)")
    private String vendorSource;

    @AvroField(name = "htmlCopyright")
    @EsField(name = "html.copyright")
    @TableField(name = "html_copyright", comment = "http请求返回copyright")
    private String htmlCopyright;

    @AvroField(name = "htmlKeywords")
    @EsField(name = "html.keywords")
    @TableField(name = "html_keywords", comment = "http请求返回关键字")
    private String htmlKeywords;

    @AvroField(name = "htmlAuthor")
    @EsField(name = "html.author")
    @TableField(name = "html_author", comment = "http请求返回作者信息")
    private String htmlAuthor;

    @AvroField(name = "htmlDescription")
    @EsField(name = "html.description")
    @TableField(name = "html_description", comment = "http请求返回描述信息")
    private String htmlDescription;

    @AvroField(name = "htmlTitle")
    @EsField(name = "html.title")
    @TableField(name = "html_title", comment = "http请求返回页面标题")
    private String htmlTitle;

    @AvroField(name = "htmlContent")
    @EsField(name = "html.content")
    @TableField(name = "html_content", comment = "http请求返回体")
    private String htmlContent;

    @AvroField(name = "model")
    @EsField(name = "model")
    @TableField(name = "model", comment = "型号")
    private String model;

    @AvroField(name = "nmapHostname")
    @EsField(name = "nmap.hostname")
    @TableField(name = "nmap_hostname", comment = "namp扫描主机名称")
    private String nmapHostname;

    @AvroField(name = "nmapProduct")
    @EsField(name = "nmap.product")
    @TableField(name = "nmap_product", comment = "namp扫描产品")
    private String nmapProduct;

    @AvroField(name = "nmapProtocol")
    @EsField(name = "nmap.protocol")
    @TableField(name = "nmap_protocol", comment = "namp扫描协议")
    private String nmapProtocol;

    @AvroField(name = "nmapServiceCpe")
    @EsField(name = "nmap.service_cpe")
    @TableField(name = "nmap_servicecpe", comment = "namp扫描cpe认证信息")
    private String nmapServiceCpe;

    @AvroField(name = "nmapOstype")
    @EsField(name = "nmap.ostype")
    @TableField(name = "nmap_ostype", comment = "namp扫描操作系统")
    private String nmapOstype;

    @AvroField(name = "nmapState")
    @EsField(name = "nmap.state")
    @TableField(name = "nmap_state", comment = "namp扫描状态(open,filter..)")
    private String nmapState;

    @AvroField(name = "nmapVersion")
    @EsField(name = "nmap.version")
    @TableField(name = "nmap_version", comment = "namp扫描产品版本号")
    private String nmapVersion;

    @AvroField(name = "nmapPortState")
    @EsField(name = "nmap.port_state")
    @TableField(name = "nmap_portstate", comment = "namp扫描端口状态")
    private String nmapPortState;

    @AvroField(name = "res")
    @EsField(name = "res")
    @TableField(name = "res", comment = "http请求返回值")
    private String res;

    @AvroField(name = "product")
    @EsField(name = "product")
    @TableField(name = "product", comment = "产品")
    private String product;

    @AvroField(name = "productVersion")
    @EsField(name = "product_version")
    @TableField(name = "product_version", comment = "产品版本号")
    private String productVersion;

    @AvroField(name = "os")
    @EsField(name = "os")
    @TableField(name = "os", comment = "操作系统")
    private String os;

    @AvroField(name = "host")
    @EsField(name = "host")
    @TableField(name = "host", comment = "主机地址信息")
    private String host;

    @AvroField(name = "ip")
    @EsField(name = "ip")
    @TableField(name = "ip", comment = "ip地址")
    private String ip;

    @AvroField(name = "module")
    @EsField(name = "module")
    @TableField(name = "module", comment = "型号")
    private String module;

    @AvroField(name = "serialNumber")
    @EsField(name = "serial_number")
    @TableField(name = "serial_number", comment = "序列号")
    private String serialNumber;

    @AvroField(name = "productName")
    @EsField(name = "product_name")
    @TableField(name = "product_name", comment = "产品名称")
    private String productName;

    @AvroField(name = "version")
    @EsField(name = "version")
    @TableField(name = "version", comment = "设备厂商")
    private String version;

    @AvroField(name = "dataSource")
    @EsField(name = "dataSource")
    @TableField(name = "data_source", comment = "数据来源")
    private String dataSource;

    @AvroField(name = "url")
    @EsField(name = "url")
    @TableField(name = "url", comment = "url链接")
    private String url;

    @AvroField(name = "port")
    @EsField(name = "port")
    @TableField(name = "port", type = "int", comment = "端口")
    private String port;

    @AvroField(name = "service")
    @EsField(name = "service")
    @TableField(name = "service", comment = "服务(应用层服务)")
    private String service;

    @AvroField(name = "serviceDesc")
    @EsField(name = "service_desc")
    @TableField(name = "service_desc", comment = "服务描述信息")
    private String serviceDesc;


    //设备信息
    @AvroField(name = "deviceSecondaryName")
    @EsField(name = "device.secondary.name")
    @TableField(name = "device_secondary_name", comment = "设备二级分类英文名称")
    private String deviceSecondaryName;

    @AvroField(name = "deviceSecondaryNameCn")
    @EsField(name = "device.secondary.namecn")
    @TableField(name = "device_secondary_namecn", comment = "设备二级分类中文名称")
    private String deviceSecondaryNameCn;

    @AvroField(name = "deviceSecondaryDesc")
    @EsField(name = "device.secondary.desc")
    @TableField(name = "device_secondary_desc", comment = "设备二级分类描述")
    private String deviceSecondaryDesc;

    @AvroField(name = "deviceThirdName")
    @EsField(name = "device.third.name")
    @TableField(name = "device_third_name", comment = "设备三级分类英文名称")
    private String deviceThirdName;

    @AvroField(name = "deviceThirdNamecn")
    @EsField(name = "device.third.namecn")
    @TableField(name = "device_third_namecn", comment = "设备三级分类中文名称")
    private String deviceThirdNamecn;

    @AvroField(name = "deviceThirdDesc")
    @EsField(name = "device.third.desc")
    @TableField(name = "device_third_desc", comment = "设备三级分类描述")
    private String deviceThirdDesc;

    @AvroField(name = "devicePrimaryName")
    @EsField(name = "device.primary.name")
    @TableField(name = "device_primary_name", comment = "设备一级分类英文名称")
    private String devicePrimaryName;

    @AvroField(name = "devicePrimaryNameCn")
    @EsField(name = "device.primary.namecn")
    @TableField(name = "device_primary_namecn", comment = "设备一级分类中文名称")
    private String devicePrimaryNameCn;

    @AvroField(name = "devicePrimaryDesc")
    @EsField(name = "device.primary.desc")
    @TableField(name = "device_primary_desc", comment = "设备一级分类描述")
    private String devicePrimaryDesc;


    //运营商信息
    @EsField(name = "ipoper.ip_unit")
    @TableField(name = "ipoper_ipunit", comment = "使用单位(企业或者个人)")
    private String ipoperIpUnit;

    @EsField(name = "ipoper.city")
    @TableField(name = "ipoper_city", comment = "城市编码")
    private String ipoperCity;

    @EsField(name = "ipoper.city_name")
    @TableField(name = "ipoper_cityname", comment = "城市名称")
    private String ipoperCityName;

    @EsField(name = "ipoper.province")
    @TableField(name = "ipoper_province", comment = "省份编码")
    private String ipoperProvince;

    @EsField(name = "ipoper.province_name")
    @TableField(name = "ipoper_provincename", comment = "省份名称")
    private String ipoperProvinceName;

    @EsField(name = "ipoper.country_code")
    @TableField(name = "ipoper_countrycode", comment = "国家编码")
    private String ipoperCountryCode;

    @EsField(name = "ipoper.country_name")
    @TableField(name = "ipoper_countryname", comment = "国家名称")
    private String ipoperCountryName;

    @EsField(name = "ipoper.idc")
    @TableField(name = "ipoper_idc", comment = "idc")
    private String ipoperIdc;

    @EsField(name = "ipoper.related_fields")
    @TableField(name = "ipoper_relatedfields", comment = "基础资源关联字段信息")
    private String ipoperRelatedFields;

    @EsField(name = "ipoper.operator_name")
    @TableField(name = "ipoper_operatorname", comment = "ip运营商")
    private String ipoperOperatorName;

    @EsField(name = "ipoper.access_name")
    @TableField(name = "ipoper_accessname", comment = "")
    private String ipoperAccessName;

    @EsField(name = "ipoper.idd_code")
    @TableField(name = "ipoper_iddcode", comment = "国际编码")
    private String ipoperIddCode;

    @EsField(name = "ipoper.detail_area")
    @TableField(name = "ipoper_detailarea", comment = "详细地址信息")
    private String ipoperDetailArea;

    @EsField(name = "ipoper.longitude")
    @TableField(name = "ipoper_longitude", comment = "经度")
    private String ipoperLongitude;

    @EsField(name = "ipoper.latitude")
    @TableField(name = "ipoper_latitude", comment = "维度")
    private String ipoperLatitude;

    @EsField(name = "ipoper.using_unit.lxr_dzyj")
    @TableField(name = "ipoper_usingunit_lxrdzyj", comment = "基础资源使用单位性质")
    private String ipoperUsingUnitLxrDzyj;

    @EsField(name = "ipoper.using_unit.tel")
    @TableField(name = "ipoper_usingunit_tel", comment = "基础资源使用单位联系电话")
    private String ipoperUsingUnitTel;

    @EsField(name = "ipoper.using_unit.linkman")
    @TableField(name = "ipoper_usingunit_linkman", comment = "基础资源使用单位联系人")
    private String ipoperUsingUnitLinkman;

    @EsField(name = "ipoper.using_unit.type")
    @TableField(name = "ipoper_usingunit_type", comment = "基础资源使用单位类型")
    private String ipoperUsingUnitType;

    @EsField(name = "ipoper.using_unit.industry")
    @TableField(name = "ipoper_usingunit_industry", comment = "基础资源行业分类编码")
    private String ipoperUsingUnitIndustry;

    @EsField(name = "ipoper.using_unit.industryType")
    @TableField(name = "ipoper_usingunit_industrytype", comment = "行业分类")
    private String ipoperUsingUnitIndustryType;

    //记录创建时间
    @AvroField(name = "createTime", type = {"long", "null"})
    @EsField(name = "create_time", type = "date")
    @TableField(name = "create_time", type = "timestamp", comment = "记录创建时间")
    private Date createTime;

    //处理时间
    @EsField(name = "process_time", type = "date")
    @TableField(name = "process_time", type = "timestamp", comment = "数据处理时间")
    private Date processTime;

    //es清洗最后一次更新时间
    @EsField(name = "update_time", type = "date")
    private Date updateTime;

    //es清洗事件发生数量
    @EsField(name = "counter")
    private Long counter;
    
    //是否有漏洞
    @EsField(name = "loophole")
    private String loophole;
    
    //banner
    @EsField(name = "banner")
    private String banner;
    
    /**
     * 资产探测hive表的分区字段
     */
    @TableField(name = "ds", comment = "年月日分区字段(yyyy-MM-dd)")
    private String ds;
}
