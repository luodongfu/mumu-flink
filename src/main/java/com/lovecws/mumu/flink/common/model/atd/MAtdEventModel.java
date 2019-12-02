package com.lovecws.mumu.flink.common.model.atd;

import com.lovecws.mumu.flink.common.annotation.EsField;
import com.lovecws.mumu.flink.common.annotation.TableField;
import com.lovecws.mumu.flink.common.annotation.TablePartition;
import com.lovecws.mumu.flink.common.annotation.TableProperties;
import lombok.Data;


/**
 * 部系统atd安全事件模型
 */
@Data
@TableProperties(storage = "parquet")
@TablePartition(partition = true, partitionType = "", partitionFields = {"event_type_id", "ds"}, databaseType = "hive")
public class MAtdEventModel extends AtdEventModel {
    
    @TableField(name = "cloud_platform_name", comment = "云平台名称")
    @EsField(name = "cloudPlatformName")
    private String cloudPlatformName;
    
}
