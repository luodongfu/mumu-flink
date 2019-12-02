package com.lovecws.mumu.flink.common.model.attack;

import com.lovecws.mumu.flink.common.annotation.TableField;
import com.lovecws.mumu.flink.common.annotation.TablePartition;
import com.lovecws.mumu.flink.common.annotation.TableProperties;
import com.baomidou.mybatisplus.extension.activerecord.Model;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * @program: act-able
 * @description: 报警数据模型
 * @author: 甘亮
 * @create: 2019-06-13 09:37
 **/
@Data
@TableProperties(storage = "parquet")
@TablePartition(partition = true, partitionType = "", partitionFields = {"ds"}, databaseType = "hive")
public class AttackEventModel extends Model<AttackEventModel> implements Serializable {


    /**
     * 第三方提供的数据模型
     */
    //源IP的业务类型，数据类型为 源IP的业务类型，数据类型为：string，信息对应表没有或对应不上为空
    @TableField(name = "source_business", comment = "源IP的业务类型")
    private String SourceBusiness;
    //源IP
    @TableField(name = "source_ip", comment = "源IP")
    private String SourceIP;
    //源端口号
    @TableField(name = "source_port", comment = "源端口号")
    private int SourcePort;
    //源IP的公司名字
    @TableField(name = "source_company", comment = "源IP的公司名字")
    private String SourceCompany;

    //目的IP
    @TableField(name = "dest_ip", comment = "目的IP")
    private String DestIP;
    //目的端口号
    @TableField(name = "dest_port", comment = "目的端口号")
    private int DestPort;
    //目的IP的业务类型，数据类型为
    @TableField(name = "dest_business", comment = "目的IP的业务类型")
    private String DestBusiness;
    //目的IP的公司名字
    @TableField(name = "dest_company", comment = "目的IP的公司名字")
    private String DestCompany;

    //协议类型
    @TableField(name = "protocol_type", comment = "协议类型")
    private String ProtocolType;
    //报警原始数据
    @TableField(name = "source_data", comment = "报警原始数据")
    private String SourceData;
    //详细协议类型
    @TableField(name = "detail_protocol", comment = "详细协议类型")
    private String DetailProtocol;
    //原始数据长度
    @TableField(name = "data_size", type = "int", comment = "原始数据长度")
    private int DataSize;
    //报警时间
    @TableField(name = "data_time", comment = "报警时间")
    private String DataTime;
    //原始数据类型 原始数据类型，int,0表示一般的数据，1表示攻击数据，2表示工业协议数据，3表示工业系统数据，4白名单，5 工业协议攻击数据
    @TableField(name = "data_type", type = "int", comment = "原始数据类型(0表示一般的数据，1表示攻击数据，2表示工业协议数据，3表示工业系统数据，4白名单，5 工业协议攻击数据)")
    private int DataType;
    //报警数据内容
    @TableField(name = "data_body", comment = "报警数据内容")
    private String DataBody;
    ///协议品牌
    @TableField(name = "protcol_brand", comment = "协议品牌")
    private String ProtcolBrand;
    //功能码
    @TableField(name = "func_code", comment = "功能码")
    private String FuncCode;
    //是否为写
    @TableField(name = "is_write", type = "boolean", comment = "是否为写")
    private boolean IsWrite;
    //设备类型 string,PLC,CNC,注塑机，机器人等类型
    @TableField(name = "device_type", comment = "设备类型(PLC,CNC,注塑机，机器人等类型)")
    private String DeviceType;
    //是否有效
    @TableField(name = "is_valid", type = "boolean", comment = "是否有效")
    private boolean IsValid;

    //TPKI 版本号 3 PUDType 1 请求->功能码:5 写变量:地址数量 1
    @TableField(name = "descript", comment = "TPKI 版本号 3 PUDType 1 请求->功能码:5 写变量:地址数量 1")
    private String Descript;
    //攻击说明
    @TableField(name = "attacks_descript", comment = "攻击说明")
    private String AttacksDescript;
    //攻击类别说明
    @TableField(name = "attacks_category", comment = "攻击类别说明")
    private String AttacksCategory;
    //攻击级别 攻击级别是1-10，1-3是低级，4-6是中级，7-10是高级
    @TableField(name = "attack_level", type = "int", comment = "攻击级别(攻击级别是1-10，1-3是低级，4-6是中级，7-10是高级)")
    private int AttackLevel;

    /**
     * 业务拓展字段
     */
    //主键
    @TableField(name = "id", comment = "id,对单条消息计算md5")
    private String id;
    //创建时间
    @TableField(name = "create_time", type = "timestamp", comment = "处理程序记录时间")
    private Date createTime;

    @TableField(name = "source_ip_value", type = "bigint", comment = "源ip的值")
    private long sourceIpValue;
    @TableField(name = "source_ip_type", comment = "源ip的类型(ipv4,ipv6)")
    private String sourceIpType;
    @TableField(name = "source_ip_country", comment = "源ip国家")
    private String sourceIpCountry;
    @TableField(name = "source_ip_country_code", comment = "源ip国家编码")
    private String sourceIpCountryCode;
    @TableField(name = "source_ip_province", comment = "源ip省份")
    private String sourceIpProvince;
    @TableField(name = "source_ip_province_code", comment = "源ip省份编码")
    private String sourceIpProvinceCode;
    @TableField(name = "source_ip_city", comment = "源ip城市")
    private String sourceIpCity;
    @TableField(name = "source_ip_city_code", comment = "源ip城市编码")
    private String sourceIpCityCode;
    @TableField(name = "source_ip_operator", comment = "源ip运营商")
    private String sourceIpOperator;
    @TableField(name = "source_ip_longitude", comment = "源ip经度")
    private String sourceIpLongitude;
    @TableField(name = "source_ip_latitude", comment = "源ip维度")
    private String sourceIpLatitude;
    @TableField(name = "source_ip_idc", comment = "源ip的idc")
    private String sourceIpIdc;
    @TableField(name = "source_industry_type", comment = "源ip行业分类")
    private String sourceIndustryType;

    @TableField(name = "dest_ip_value", type = "bigint", comment = "目的ip的值")
    private long destIpValue;
    @TableField(name = "dest_ip_type", comment = "目的ip类型(ipv4,ipv6)")
    private String destIpType;
    @TableField(name = "dest_ip_country", comment = "目的ip国家")
    private String destIpCountry;
    @TableField(name = "dest_ip_country_code", comment = "目的ip国家编码")
    private String destIpCountryCode;
    @TableField(name = "dest_ip_province", comment = "目的ip省份")
    private String destIpProvince;
    @TableField(name = "dest_ip_province_code", comment = "目的ip省份编码")
    private String destIpProvinceCode;
    @TableField(name = "dest_ip_city", comment = "目的ip城市名称")
    private String destIpCity;
    @TableField(name = "dest_ip_city_code", comment = "目的ip城市编码")
    private String destIpCityCode;
    @TableField(name = "dest_ip_operator", comment = "目的ip运营商")
    private String destIpOperator;
    @TableField(name = "dest_ip_longitude", comment = "目的ip经度")
    private String destIpLongitude;
    @TableField(name = "dest_ip_latitude", comment = "目的ip维度")
    private String destIpLatitude;
    @TableField(name = "dest_ip_idc", comment = "目的ip的idc")
    private String destIpIdc;
    @TableField(name = "dest_industry_type", comment = "目的行业分类")
    private String destIndustryType;

    @TableField(name = "ds", comment = "年月日分区字段(yyyy-MM-dd)")
    private String ds;

    /**
     * 2019-09-26新添加字段
     */
    @TableField(name = "attack_id", comment = "攻击id")
    private String AttackID;
    //攻击ID，10位数字，前2位表示大类，18表示工业设备攻击，02表示西门子，02表示写数据攻击,0001唯一标识
    @TableField(name = "threat_id", comment = "攻击ID，10位数字，前2位表示大类，18表示工业设备攻击，02表示西门子，02表示写数据攻击,0001唯一标识")
    private String ThreatID;
    //标准的CVEID
    @TableField(name = "cveid", comment = "标准的CVEID")
    private String CVEID;
    //受威胁的参考方案
    @TableField(name = "threat_solution", comment = "受威胁的参考方案")
    private String ThreatSolution;
    //源设备品牌
    @TableField(name = "source_brand", comment = "源设备品牌")
    private String SourceBrand;
    //源网卡MAC地址
    @TableField(name = "source_mac_address", comment = "源网卡MAC地址")
    private String SourceMacAddress;
    //目标设备品牌
    @TableField(name = "dest_brand", comment = "目标设备品牌")
    private String DestBrand;
    //目标网卡MAC地址
    @TableField(name = "dest_mac_address", comment = "目标网卡MAC地址")
    private String DestMacAddress;
}
