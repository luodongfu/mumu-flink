package com.lovecws.mumu.flink.common.model.dpi;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.extension.activerecord.Model;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;

/**
 * @Description: 协议特征库实体类
 * @author: lyc
 * @date: 2019-6-26 17:36
 */
@Data
@EqualsAndHashCode(callSuper = false)
@TableName(value = "tb_protocol_device")
public class ProtocolDevice extends Model<ProtocolDevice> {
	
	private static final long serialVersionUID = 1L;

	@TableId(value = "id",type = IdType.AUTO)
    private Long id;

    private String service;

    private String primaryNamecn;

    private String secondaryNamecn;

    private String vendor;
    
    private String serviceDesc;

    private String ports;

    private Date createTime;
    
    private String cloudPlatform;
    
    private Integer type;

}
