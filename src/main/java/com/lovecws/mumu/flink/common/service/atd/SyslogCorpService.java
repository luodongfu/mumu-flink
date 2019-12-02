package com.lovecws.mumu.flink.common.service.atd;

import com.lovecws.mumu.flink.common.mapper.BaseServiceImpl;
import com.lovecws.mumu.flink.common.mapper.atd.SyslogCorpMapper;
import com.lovecws.mumu.flink.common.model.atd.SyslogCorpModel;

import java.util.Map;

public class SyslogCorpService extends BaseServiceImpl<SyslogCorpMapper, SyslogCorpModel> {

    public Map<String, Object> getCorpInfo(Long ipValue) {
        return baseMapper.getCorpInfo(ipValue);
    }

    // BD：bigdata，从大数据来的数据
    public Map<String, Object> getCorpInfoFromBD(Long ipValue) {
        return baseMapper.getCorpInfoFromBD(ipValue);
    }
}
