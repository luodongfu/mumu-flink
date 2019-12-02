package com.lovecws.mumu.flink.streaming.task.gynetres;

import com.alibaba.fastjson.JSON;
import com.lovecws.mumu.flink.common.model.loophole.CnvdModel;
import com.lovecws.mumu.flink.common.service.loophole.CnvdService;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.List;

/**
 * @program: trunk
 * @description: 資產探測任務測試
 * @author: 甘亮
 * @create: 2019-08-27 09:00
 **/
public class GynetresEventTaskTest {

    private static final Logger log = Logger.getLogger(GynetresEventTaskTest.class);
    private CnvdService cnvdService=new CnvdService();

    @Test
    public void queryCnvdRefectProduct() {
        //List<CnvdModel> cnvdModelList = cnvdService.queryCnvdRefectProduct("Apache httpd", "2.4.25");
        //List<CnvdModel> cnvdModelList = cnvdService.queryCnvdRefectProduct("Apache Tomcat/Coyote JSP engine", "1.1");
        //List<CnvdModel> cnvdModelList = cnvdService.queryCnvdRefectProduct("Microsoft IIS httpd", "7.5");
        List<CnvdModel> cnvdModelList = cnvdService.queryCnvdRefectProduct("lighttpd", "1.4.32");
        cnvdModelList.forEach(cnvdModel -> log.info(JSON.toJSONString(cnvdModel)));
    }
}
