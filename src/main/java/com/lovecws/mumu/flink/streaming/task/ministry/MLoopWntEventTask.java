package com.lovecws.mumu.flink.streaming.task.ministry;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.model.gynetres.MinistryLoopholeModel;
import com.lovecws.mumu.flink.common.service.gynetres.MinistryLoopholeService;
import com.lovecws.mumu.flink.common.util.TxtUtils;
import com.lovecws.mumu.flink.streaming.task.AbstractStreamingTask;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Author: wucw
 * @Date: 2019-7-5 9:47
 * @Description: 部dpi数据处理
 * （kafka获取数据 -> 数据处理 -> 数据入Es -> 数据入hive）
 */
@Slf4j
@ModularAnnotation(type = "task", name = "mloopwnt")
public class MLoopWntEventTask extends AbstractStreamingTask {

    private static final long serialVersionUID = 1L;
    private String[] storageFields;

    public MLoopWntEventTask(Map<String, Object> configMap) {
        super(configMap);
        storageFields = MapUtils.getString(configMap, "fields", "").split(",");
    }

    public Object parseData(Object data) {
        MinistryLoopholeModel mlm = new MinistryLoopholeModel();
        String line = new String((byte[]) data, StandardCharsets.UTF_8);
        // 每行数据采用 #|# 分隔各字段,如果字段是数组,采用 @#@分隔每个元素
        TxtUtils.transforModel(line, storageFields, mlm, "yyyy-MM-dd", "#\\|#");
        return mlm;
    }

    @Override
    public Object fillData(Object data) {
        matchLoophole(data);
        return data;
    }

    @Override
    public Boolean filterData(Object data) {
        return true;
    }

    private void matchLoophole(Object data) {
        MinistryLoopholeService ministryLoopholeService = configManager.getBean(MinistryLoopholeService.class);
        List<MinistryLoopholeModel> loopholeModels = new ArrayList<>();
        MinistryLoopholeModel loophole = (MinistryLoopholeModel) data;
        if (loophole.getCnnvdNo() != null) {
            loophole.setCnnvdNo(loophole.getCnnvdNo().replace(" ", ""));
        }
        if (loophole.getCnvdNo() != null) {
            loophole.setCnvdNo(loophole.getCnvdNo().replace(" ", ""));
        }
        if (loophole.getCveNo() != null) {
            loophole.setCveNo(loophole.getCveNo().replace(" ", ""));
        }
        loopholeModels.add(loophole);
        if (loopholeModels.size() > 0) {
            ministryLoopholeService.saveBatch(loopholeModels, 1000);
        }
    }
}
