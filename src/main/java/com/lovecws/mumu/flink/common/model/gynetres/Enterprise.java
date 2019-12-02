package com.lovecws.mumu.flink.common.model.gynetres;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.extension.activerecord.Model;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @Description: 企业表实体类
 * @author: wucw
 * @date: 2019-7-31 17:36
 */
@Data
@EqualsAndHashCode(callSuper = false)
@TableName(value = "tb_enterprise")
public class Enterprise extends Model<Enterprise> {
	
	private static final long serialVersionUID = 1L;

	@TableId(value = "id",type = IdType.AUTO)
	private Long id;
	private String companyname;
	private Long beginipvalue;
	private Long endipvalue;
	private String subIndustryName;
	private String companytypecategory;
	
//	private String companytype;
//	private String operatescope;
//	private String beginip;
//	private String endip;
//	private String domain;
//	private String firstrecordtime;
//	private String lasttime;
//	private String legalname;
//	private String registcode;
//	private String registaddress;
//	private String registorgname;
//	private String approvaltime;
//	private String enterprisestatus;
//	private String registmoney;
//	private String orgcode;
//	private String establishtime;
//	private String starttime;
//	private String endtime;
//	private String approvalcompany;
//	private String listedcode;
//	private String creditcode;
//	private String recordnumber;
//	private String classification;
//	private String assetcount;
//	private String citycode;
//	private String cityname;
//	private String registercapital;
//	private String province_code;
//	private String province_name;
//	private String industry_name;
//	private String industry_code;
//	private String isimportant;
//	private String iscloud;
//	private String areaname;
//	private String country;
//	private String operators;
//	private String linkman;
//	private String linkphone;
//	private String visit_count;
//	private String siteid;
//	private String create_time;
//	private String is_handle;
//	private String unit_id;
//	private String cloud_org;
//	private String sub_industry_code;
//	private String email;
//	private String uploadprovincecode;
//	private String uploadprovincename;
//	private String datasource;

}
