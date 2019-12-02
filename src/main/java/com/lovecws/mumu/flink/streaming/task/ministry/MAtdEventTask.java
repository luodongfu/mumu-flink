package com.lovecws.mumu.flink.streaming.task.ministry;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.model.atd.MAtdEventModel;
import com.lovecws.mumu.flink.common.service.atd.CloudPlatformService;
import com.lovecws.mumu.flink.common.util.DateUtils;
import com.lovecws.mumu.flink.common.util.IPUtil;
import com.lovecws.mumu.flink.common.util.TxtUtils;
import com.lovecws.mumu.flink.streaming.common.cache.MCorpCache;
import com.lovecws.mumu.flink.streaming.common.handler.HubeiHandler;
import com.lovecws.mumu.flink.streaming.common.handler.IpipnetHandler;
import com.lovecws.mumu.flink.streaming.task.AbstractStreamingTask;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @Author: wucw
 * @Date: 2019-7-5 9:47
 * @Description: 部系统atd数据处理
 */
@Slf4j
@ModularAnnotation(type = "task", name = "matd")
public class MAtdEventTask extends AbstractStreamingTask {

    private static final long serialVersionUID = 1L;
    //被攻击方事件类型(目的ip攻击源ip事件类型)
    private static final List<String> ATTACKED_EVENT_TYPES = Arrays.asList("1", "6", "3", "13", "15", "16");
    //被攻击方事件二级分类(目的ip攻击源ip事件类型)
    private static final List<String> ATTACKED_EVENT_SUBCATEGORY = Arrays.asList("特洛伊木马通信", "恶意加密流量");

    private MCorpCache mCorpCache;
    private int srcIpFind = 0;
    private int dstIpFind = 0;

    private IpipnetHandler ipipnetHandler;
    private HubeiHandler hubeiHandler;

    private String[] storageFields;
    public MAtdEventTask(Map<String, Object> configMap) {
        super(configMap);
        ipipnetHandler = new IpipnetHandler();
        hubeiHandler = new HubeiHandler();
        mCorpCache = new MCorpCache();
        storageFields = MapUtils.getString(configMap, "fields", "").split(",");
    }

    @Override
    public Object parseData(Object data) {

        MAtdEventModel atd = new MAtdEventModel();
        String line = new String((byte[]) data, StandardCharsets.UTF_8);

        TxtUtils.transforModel(line, storageFields, FieldUtils.getAllFields(atd.getClass()), atd, "yyyy-MM-dd HH:mm:ss", "\\|");

        // 处理ds
        Date timestamp = atd.getTimestamp();
        if (timestamp == null) timestamp = new Date();
        atd.setDs(DateUtils.formatDate(timestamp, "yyyy-MM-dd"));

        return atd;
    }

    @Override
    public Object fillData(Object data) {
        MAtdEventModel atd = (MAtdEventModel) data;
        try {
            CloudPlatformService cloudPlatformService = new CloudPlatformService();

            // 获取云平台名称
            String attackedIp = atd.getAttackedIp();
            if (StringUtils.isNotBlank(attackedIp)) {
                String name = cloudPlatformService.getCloudPlatformNameByIp(IPUtil.ipToLong(attackedIp));
                atd.setCloudPlatformName(name);
            }
            // 补充源ip、目的ip、攻击ip、被攻击ip的企业名臣、企业性质等
            supplyCorp(atd);
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        }
        return atd;
    }

    private void supplyCorp(MAtdEventModel atd) {
        try {
            String srcCorpType = null, dstCorpType = null;
            boolean srcChangeFlag = false, dstChangeFlag = false;
            // 重新查找源ip的企业名称
            if (StringUtils.isBlank(atd.getSrcCorpName()) && StringUtils.isNotBlank(atd.getSrcIp())) {
                Map<String, Object> srcCorpInfo = mCorpCache.getCorpInfoFromDB(atd.getSrcIpValue());
                atd.setSrcCorpName(srcCorpInfo.getOrDefault("corpname", "").toString());
                atd.setSrcIndustry(srcCorpInfo.getOrDefault("industry", "").toString());
                srcCorpType = srcCorpInfo.getOrDefault("corptype", "").toString();
                srcChangeFlag = true;

                // 统计使用
                if (StringUtils.isNotBlank(atd.getSrcCorpName())) {
                    srcIpFind++;
                }
            }
            // 重新查找目的ip的企业名称
            if (StringUtils.isBlank(atd.getCorpName()) && StringUtils.isNotBlank(atd.getDstIp())) {
                Map<String, Object> corpInfo = mCorpCache.getCorpInfoFromDB(atd.getDstIpValue());
                atd.setCorpName(corpInfo.getOrDefault("corpname", "").toString());
                atd.setIndustry(corpInfo.getOrDefault("industry", "").toString());
                dstCorpType = corpInfo.getOrDefault("corptype", "").toString();
                dstChangeFlag = true;

                // 统计使用
                if (StringUtils.isNotBlank(atd.getCorpName())) {
                    dstIpFind++;
                }
            }
            // 攻击方 被攻击方
            String eventTypeId = atd.getEventTypeId();
            String subCategory = atd.getCategory();
            // 目的ip是攻击源ip事件类型
            if (ATTACKED_EVENT_TYPES.contains(eventTypeId) || ATTACKED_EVENT_SUBCATEGORY.contains(subCategory)) {
                if (dstChangeFlag) {
                    atd.setAttackCorpName(atd.getCorpName());
                    atd.setAttackIndustry(atd.getIndustry());
                    if ((StringUtils.isNotBlank(dstCorpType))) {
                        atd.setAttackCorpType(dstCorpType);
                    }
                }
                if (srcChangeFlag) {
                    atd.setAttackedCorpName(atd.getSrcCorpName());
                    atd.setAttackedIndustry(atd.getSrcIndustry());
                    if ((StringUtils.isNotBlank(srcCorpType))) {
                        atd.setAttackedCorpType(srcCorpType);
                    }
                }

            }
            // 源ip攻击目的ip事件类型
            else {
                if (srcChangeFlag) {
                    atd.setAttackCorpName(atd.getSrcCorpName());
                    atd.setAttackIndustry(atd.getSrcIndustry());
                    if ((StringUtils.isNotBlank(srcCorpType))) {
                        atd.setAttackCorpType(srcCorpType);
                    }
                }

                if (dstChangeFlag) {
                    atd.setAttackedCorpName(atd.getCorpName());
                    atd.setAttackedIndustry(atd.getIndustry());
                    if ((StringUtils.isNotBlank(dstCorpType))) {
                        atd.setAttackedCorpType(dstCorpType);
                    }
                }

            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        }
    }

    @Override
    public Boolean filterData(Object data) {
        return true;
    }
}
