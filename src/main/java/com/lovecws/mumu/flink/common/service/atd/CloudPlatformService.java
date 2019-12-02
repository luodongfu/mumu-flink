package com.lovecws.mumu.flink.common.service.atd;

import com.lovecws.mumu.flink.common.mapper.BaseServiceImpl;
import com.lovecws.mumu.flink.common.mapper.atd.CloudPlatformMapper;
import com.lovecws.mumu.flink.common.model.atd.SyslogCorpModel;

public class CloudPlatformService extends BaseServiceImpl<CloudPlatformMapper, SyslogCorpModel> {

    public String getCloudPlatformNameByIp(long ip) {
        return baseMapper.getCloudPlatformNameByIp(ip);
    }
}
