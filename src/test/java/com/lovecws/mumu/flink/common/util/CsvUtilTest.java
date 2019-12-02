package com.lovecws.mumu.flink.common.util;

import com.lovecws.mumu.flink.common.model.attack.AttackEventModel;
import org.junit.Test;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;

/**
 * @program: act-able
 * @description: TODO
 * @author: 甘亮
 * @create: 2019-06-18 19:20
 **/
public class CsvUtilTest {

    @Test
    public void writeCSVFile() {
        AttackEventModel attackEventModel = new AttackEventModel();
        attackEventModel.setCreateTime(new Date());
        attackEventModel.setAttackLevel(1);
        attackEventModel.setSourceIP("172.31.134.225");
        attackEventModel.setSourcePort(80);
        attackEventModel.setDestIP("58.20.16.24");
        attackEventModel.setDestPort(443);
        attackEventModel.setSourceCompany("湖南天汽模汽车模具技术股份有限公司");
        attackEventModel.setDestCompany("中国移动通信集团\n\r\t湖南有限公司");

        //CsvUtil.writeCSVFile(Arrays.asList(attackEventModel), "E:\\data\\dataprocessing\\util\\1.csv", '|', true);

        CsvUtil.writeFile(Arrays.asList(attackEventModel),"E:\\data\\dataprocessing\\util\\2.csv", "|");
    }

    public static void main(String[] args) {
        Calendar calendar=Calendar.getInstance();
        calendar.setTimeInMillis(1573817218560L);
        System.out.println(calendar.getTime().toLocaleString());

        calendar.setTimeInMillis(1565704678389L);
        System.out.println(calendar.getTime().toLocaleString());
    }
}
