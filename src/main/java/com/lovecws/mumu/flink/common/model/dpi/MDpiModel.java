package com.lovecws.mumu.flink.common.model.dpi;

import com.lovecws.mumu.flink.common.annotation.EsField;
import com.lovecws.mumu.flink.common.annotation.TableField;
import com.lovecws.mumu.flink.common.annotation.TablePartition;
import com.lovecws.mumu.flink.common.annotation.TableProperties;
import lombok.Data;

import java.util.Date;


/**
 * 部系统atd安全事件模型
 */
@Data
@TableProperties(storage = "parquet")
@TablePartition(partition = true, partitionType = "", partitionFields = {"ds"}, databaseType = "hive")
public class MDpiModel extends DpiModel {
    
    @TableField(name = "cloud_platform_name", comment = "云平台名称")
    @EsField(name = "cloudPlatformName")
    private String cloudPlatformName;

    // 入库时间
    @EsField(name = "insertTime", type = "date", format = "yyyy-MM-dd HH:mm:ss")
    public Date insertTime;
    
}
