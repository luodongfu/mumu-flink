package com.lovecws.mumu.flink.streaming.common.handler;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.model.atd.AtdEventModel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

/**
 * 对湖北数据进行特殊处理
 */
@Slf4j
@ModularAnnotation(type = "handler", name = "hubei")
public class HubeiHandler implements Serializable {

    // 湖北数据，如果ip是10开头且city为空，则赋值为武汉
    public void handle4atd(AtdEventModel atdModel) {
        if (strValueJudge(atdModel.getUploadProvinceCode(), "420000", 0)) {
            if (strValueJudge(atdModel.getSrcIp(), "10", 1)) {
                atdModel.setSrcIpCountry("中国");
                atdModel.setSrcIpProvince("湖北省");
                atdModel.setSrcIpCity("武汉市");
            }
            if (strValueJudge(atdModel.getDstIp(), "10", 1)) {
                atdModel.setDstIpCountry("中国");
                atdModel.setDstIpProvince("湖北省");
                atdModel.setDstIpCity("武汉市");
            }
            if (strValueJudge(atdModel.getAttackIp(), "10", 1)) {
                atdModel.setAttackCountry("中国");
                atdModel.setAttackProvince("湖北省");
                atdModel.setAttackCity("武汉市");
            }
            if (strValueJudge(atdModel.getAttackedIp(), "10", 1)) {
                atdModel.setAttackedCountry("中国");
                atdModel.setAttackedProvince("湖北省");
                atdModel.setAttackedCity("武汉市");
            }
        }

    }

    // type为0默认判断，type为1代表startsWith
    private boolean strValueJudge(String value, String judgeValue, int type) {
        if (type == 1) {
            return StringUtils.isNotBlank(value) && value.startsWith(judgeValue);
        }
        return StringUtils.isNotBlank(value) && value.equals(judgeValue);
    }
}
