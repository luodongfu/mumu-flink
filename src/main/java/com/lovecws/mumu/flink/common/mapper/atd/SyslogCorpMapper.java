package com.lovecws.mumu.flink.common.mapper.atd;

import com.lovecws.mumu.flink.common.model.atd.SyslogCorpModel;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;

import java.util.Map;

public interface SyslogCorpMapper extends BaseMapper<SyslogCorpModel> {

    Map<String,Object> getCorpInfo(@Param(value = "ip") Long ipValue);

    Map<String,Object> getCorpInfoFromBD(@Param(value = "ip") Long ipValue);
}
