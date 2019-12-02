package com.lovecws.mumu.flink.streaming.task.ministry;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.model.dpi.MDpiModel;
import com.lovecws.mumu.flink.common.service.atd.CloudPlatformService;
import com.lovecws.mumu.flink.common.util.DateUtils;
import com.lovecws.mumu.flink.common.util.IPUtil;
import com.lovecws.mumu.flink.common.util.TxtUtils;
import com.lovecws.mumu.flink.streaming.task.AbstractStreamingTask;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;

/**
 * @Author: wucw
 * @Date: 2019-7-5 9:47
 * @Description: 部dpi数据处理
 * （kafka获取数据 -> 数据处理 -> 数据入Es -> 数据入hive）
 */
@Slf4j
@ModularAnnotation(type = "task", name = "mdpi")
public class MDpiEventTask extends AbstractStreamingTask {

    private static final long serialVersionUID = 1L;
    private String[] storageFields;

    public MDpiEventTask(Map<String, Object> configMap) {
        super(configMap);
        storageFields = MapUtils.getString(configMap, "fields", "").split(",");
    }

    @Override
    public Object parseData(Object data) {
        MDpiModel dpi = new MDpiModel();
        String line = new String((byte[]) data, StandardCharsets.UTF_8);
        TxtUtils.transforModel(line, storageFields, FieldUtils.getAllFields(dpi.getClass()), dpi, "yyyy-MM-dd HH:mm:ss", "\\|");
        if (dpi.getCreateTime() == null) {
            dpi.setCreateTime(new Date());
        }
        dpi.setDs(DateUtils.formatDate(dpi.getCreateTime(), "yyyy-MM-dd"));

        // 入库时间
        dpi.setInsertTime(new Date());

        return dpi;
    }

    @Override
    public Object fillData(Object data) {
        CloudPlatformService cloudPlatformService = configManager.getBean(CloudPlatformService.class);

        MDpiModel dpi = (MDpiModel) data;

        // 获取云平台名称
        String srcIp = dpi.getSrcIp();
        if (StringUtils.isNotBlank(srcIp)) {
            String name = cloudPlatformService.getCloudPlatformNameByIp(IPUtil.ipToLong(srcIp));
            dpi.setCloudPlatformName(name);
        }
        return dpi;
    }

    @Override
    public Boolean filterData(Object data) {
        return true;
    }
}
