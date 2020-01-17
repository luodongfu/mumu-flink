package com.lovecws.mumu.flink.streaming.service;

import com.lovecws.mumu.flink.common.model.gynetres.HostLoopholeModel;
import com.lovecws.mumu.flink.common.service.gynetres.HostLoopholeService;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;

/**
 * @program: mumu-flink
 * @description: ${description}
 * @author: 甘亮
 * @create: 2019-11-26 15:33
 **/
public class HostLoopholeServiceTest {

    @Test
    public void save() {
        HostLoopholeService hostLoopholeService=new HostLoopholeService();
        HostLoopholeModel hostLoopholeModel = new HostLoopholeModel();
        hostLoopholeModel.setTaskId(123);
        hostLoopholeModel.setTaskInstanceId("");
        hostLoopholeModel.setIp("172.31.134.224");
        hostLoopholeModel.setPort("80");
        hostLoopholeModel.setCreateTime(new Date());

        boolean save = hostLoopholeService.saveBatch(Arrays.asList(hostLoopholeModel));
        System.out.println(save);
    }

    @Test
    public void get() {
        HostLoopholeService hostLoopholeService = new HostLoopholeService();
        HostLoopholeModel hostLoopholeModel = hostLoopholeService.getById(567158L);
        System.out.println(hostLoopholeModel);
    }
}
