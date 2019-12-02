package com.lovecws.mumu.flink.common.model.loophole;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.extension.activerecord.Model;
import lombok.Data;

import java.io.Serializable;

/**
 * @program: act-able
 * @description: cnvd漏洞库模型
 * @author: 甘亮
 * @create: 2019-06-06 13:09
 **/
@Data
@TableName("tb_cnvd")
public class CnvdModel extends Model<CnvdModel> implements Serializable {

    @TableId(value = "id", type = IdType.INPUT)
    private String id;

    @TableField("reflect_product")
    private String reflectProduct;

    @TableField("thread")
    private String thread;

    @TableField("soft_style")
    private String softStyle;

    @TableField("serverity")
    private String serverity;

    @TableField("cnvd_no")
    private String cnvdNo;

    @TableField("description")
    private String description;
}
