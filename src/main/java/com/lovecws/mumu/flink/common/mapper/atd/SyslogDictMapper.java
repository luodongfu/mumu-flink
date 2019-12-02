package com.lovecws.mumu.flink.common.mapper.atd;

import com.lovecws.mumu.flink.common.model.atd.SyslogCorpModel;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;

import java.util.List;
import java.util.Map;

public interface SyslogDictMapper extends BaseMapper<SyslogCorpModel> {

    List<Map<Object, Object>> getThreadSeverityDict();

    List<Map<String, String>> getKillChainDict();

    List<Map<String, String>> getThreadEngineDict();

    List<Map<String, String>> getThreadTypeClassDict();

    List<Map<String, String>> getThreadTypeCategoryDict();
}
