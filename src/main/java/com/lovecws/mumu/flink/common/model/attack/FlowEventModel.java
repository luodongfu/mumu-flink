package com.lovecws.mumu.flink.common.model.attack;

import com.lovecws.mumu.flink.common.annotation.TableField;
import com.lovecws.mumu.flink.common.annotation.TablePartition;
import com.lovecws.mumu.flink.common.annotation.TableProperties;
import com.baomidou.mybatisplus.extension.activerecord.Model;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * @program: mumu-flink
 * @description: 报警数据模型
 * @author: 甘亮
 * @create: 2019-06-13 09:37
 **/
@Data
@TableProperties(storage = "parquet")
@TablePartition(partition = true, partitionType = "", partitionFields = {"ds"}, databaseType = "hive")
public class FlowEventModel extends Model<FlowEventModel> implements Serializable {


    /**
     * 第三方提供的数据模型
     */
    @TableField(name = "all_count", comment = "")
    private long AllCount;
    @TableField(name = "all_flow", comment = "")
    private long AllFlow;
    @TableField(name = "actual_all_count", comment = "")
    private long ActualAllCount;
    @TableField(name = "actual_all_flow", comment = "")
    private long ActualAllFlow;
    @TableField(name = "uuid", comment = "")
    private String uuid;
    @TableField(name = "all_count_time", type = "timestamp", comment = "")
    private Date AllCountTime;

    @TableField(name = "payload_datalist", comment = "")
    private String PayloadDataList;

    @TableField(name = "max_size", type = "bigint", comment = "最大流量大小")
    private long MaxSize;
    @TableField(name = "min_size", type = "bigint", comment = "最小流量大小")
    private long MinSize;
    @TableField(name = "average_size", type = "bigint", comment = "流量平均大小")
    private long AverageSize;
    @TableField(name = "size", type = "bigint", comment = "流量大小")
    private long Size;
    @TableField(name = "count", type = "bigint", comment = "")
    private long Count;

    @TableField(name = "protocol_name", comment = "协议名称")
    private String ProtocolName;
    @TableField(name = "detail_protocol", comment = "详细协议")
    private String DetailProtocol;

    @TableField(name = "source_business", comment = "源IP的业务类型")
    private String SourceBusiness;
    @TableField(name = "source_company", comment = "源ip公司名称")
    private String SourceCompany;
    @TableField(name = "source_ip", comment = "源ip")
    private String SourceIP;
    @TableField(name = "source_port", type = "int", comment = "源端口")
    private int SourcePort;
    @TableField(name = "source_brand", comment = "源品牌")
    private String SourceBrand;

    @TableField(name = "dest_business", comment = "目的IP的业务类型")
    private String DestBusiness;
    @TableField(name = "dest_company", comment = "目的ip公司名称")
    private String DestCompany;
    @TableField(name = "dest_ip", comment = "目的ip")
    private String DestIP;
    @TableField(name = "dest_port", type = "int", comment = "目的端口")
    private int DestPort;
    @TableField(name = "dest_brand", comment = "目的品牌")
    private String DestBrand;

    @TableField(name = "func_code", comment = "功能码")
    private String FuncCode;

    //TPKI 版本号 3 PUDType 1 请求->功能码:4 读变量:地址数量 1
    @TableField(name = "descript", comment = "TPKI 版本号 3 PUDType 1 请求->功能码:4 读变量:地址数量 1")
    private String Descript;
    @TableField(name = "is_write", type = "boolean", comment = "是否为写")
    private String IsWrite;
    @TableField(name = "data_type", type = "int", comment = "原始数据类型,0表示一般数据、1表示攻击数据、2表示工业协议数据、3表示工业系统数据、4表示白名单")
    private int DataType;

    @TableField(name = "count_time", type = "timestamp", comment = "")
    private Date CountTime;
    @TableField(name = "max_interver_time", type = "int", comment = "最大间隔时间")
    private int MaxInterverTime;
    @TableField(name = "min_interver_time", type = "int", comment = "最小间隔时间")
    private int MinInterverTime;
    @TableField(name = "average_interval_time", type = "int", comment = "平均间隔时间")
    private int AverageIntervalTime;

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
    //工业流量
    @TableField(name = "industry_flow", comment = "工业流量")
    private String IndustryFlow;

    //工业流量条数
    @TableField(name = "industry_count", comment = "工业流量条数")
    private String IndustryCount;
}
