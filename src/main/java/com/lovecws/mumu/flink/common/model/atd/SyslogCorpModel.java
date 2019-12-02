package com.lovecws.mumu.flink.common.model.atd;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.extension.activerecord.Model;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @Author: Tang Jichen
 * @Date: 2019/3/6
 * @Description: atd字典表实体模型
 */

@Data
@EqualsAndHashCode(callSuper = false)
@TableName(value = "tb_syslog_corp_dict")
public class SyslogCorpModel extends Model<SyslogCorpModel> {
    @TableId(value = "id",type = IdType.AUTO)
    private Long id;

    private String corpName;

    private String industry;

    private String beginIp;

    private String endIp;

    private Long beginIpValue;

    private Long endIpValue;

}
