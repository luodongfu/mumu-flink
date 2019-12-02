package com.lovecws.mumu.flink.common.service.atd;

import com.lovecws.mumu.flink.common.mapper.BaseServiceImpl;
import com.lovecws.mumu.flink.common.mapper.atd.SyslogDictMapper;
import com.lovecws.mumu.flink.common.model.atd.SyslogCorpModel;

import java.util.List;
import java.util.Map;

public class SyslogDictService extends BaseServiceImpl<SyslogDictMapper, SyslogCorpModel> {

    /**
     * 获取到威胁等级映射关系
     *
     * @return
     */
    public List<Map<Object, Object>> getThreadSeverityDict() {
        return baseMapper.getThreadSeverityDict();
    }

    /**
     * 获取到攻击链映射关系
     *
     * @return
     */
    public List<Map<String, String>> getKillChainDict() {
        return baseMapper.getKillChainDict();
    }

    /**
     * 获取攻击类型映射
     *
     * @return
     */
    public List<Map<String, String>> getThreadTypeClassDict() {
        return baseMapper.getThreadTypeClassDict();
    }

    /**
     * 获取攻击份分类映射
     *
     * @return
     */
    public List<Map<String, String>> getThreadTypeCategoryDict() {
        return baseMapper.getThreadTypeCategoryDict();
    }

}
