package com.lovecws.mumu.flink.streaming.common.handler;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.util.IPIPNetUtil;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 根据ip匹配运营商、地市信息
 */
@ModularAnnotation(type = "handler", name = "ipipnet")
public class IpipnetHandler implements Serializable {

    public Map<String, Object> handler(String ip) {
        if (StringUtils.isEmpty(ip)) return new HashMap<>();
        Map<String, String> stringStringMap = IPIPNetUtil.get(ip);

        Map<String, Object> resultMap = new HashMap<>();
        resultMap.putAll(stringStringMap);
        return resultMap;
    }
}
