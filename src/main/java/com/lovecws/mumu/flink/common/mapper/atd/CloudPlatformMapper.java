package com.lovecws.mumu.flink.common.mapper.atd;

import com.lovecws.mumu.flink.common.model.atd.SyslogCorpModel;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;

public interface CloudPlatformMapper extends BaseMapper<SyslogCorpModel> {

    String getCloudPlatformNameByIp(@Param(value = "ip") long ip);

}
