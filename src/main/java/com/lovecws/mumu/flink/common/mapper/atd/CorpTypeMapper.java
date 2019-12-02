package com.lovecws.mumu.flink.common.mapper.atd;

import com.lovecws.mumu.flink.common.model.atd.SyslogCorpModel;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;

public interface CorpTypeMapper extends BaseMapper<SyslogCorpModel> {

    String getEnterpriseInfo(@Param(value = "corpName") String corpName);
}
