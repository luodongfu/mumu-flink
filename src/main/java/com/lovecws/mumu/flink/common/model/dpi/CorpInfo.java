package com.lovecws.mumu.flink.common.model.dpi;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.extension.activerecord.Model;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @Description: atd专线企业实体类
 * @author: lyc
 * @date: 2019-6-26 17:36
 */
@Data
@EqualsAndHashCode(callSuper = false)
@TableName(value = "tb_syslog_corp_dict")
public class CorpInfo extends Model<CorpInfo> {
	
	private static final long serialVersionUID = 1L;

	@TableId(value = "id",type = IdType.AUTO)
    private Long id;

    private String corpName;

    private String industry;

    private String beginIp;

    private String endIp;

    private Long beginIpValue;

    private Long endIpValue;

}
