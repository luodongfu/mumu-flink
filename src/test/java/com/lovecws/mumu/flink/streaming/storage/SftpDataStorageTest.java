package com.lovecws.mumu.flink.streaming.storage;

import com.lovecws.mumu.flink.common.model.dpi.DpiModel;
import com.lovecws.mumu.flink.streaming.sink.SftpSink;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @program: mumu-flink
 * @description: atd清洗表存储测试
 * @author: 甘亮
 * @create: 2019-06-12 15:29
 **/
public class SftpDataStorageTest {

    @Test
    public void storage() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("uploadDir", "sftp/data/atd");
        configMap.put("fileType", "txt");
        configMap.put("fileds", "id,primaryId,srcIp,destIp,srcPort,destPort,service,upPackNum,upByteNum,downPackNum,downByteNum,srcCorpName,srcIndustry,destCorpName,destIndustry,dynamicField,url,data,deviceinfoPrimaryNamecn,deviceinfoSecondaryNamecn,deviceinfoVendor,deviceinfoServiceDesc,deviceinfoCloudPlatform,deviceinfoDeviceid,deviceinfoSn,deviceinfoMac,deviceinfoMd5,deviceinfoOs,deviceinfoSoftVersion,deviceinfoFirmwareVersion,deviceinfoIp,deviceinfoNumber,ipinfoCountry,ipinfoProvince,ipinfoCity,ipinfoOwner,ipinfoTimeZoneCity,ipinfoTimeZone,ipinfoAreaCode,ipinfoGlobalRoaming,ipinfoInternalCode,ipinfoStateCode,ipinfoOperator,ipinfoLongitude,ipinfoLatitude,ipinfoArea,destipinfoCountry,destipinfoProvince,destipinfoCity,destipinfoOwner,destipinfoTimeZoneCity,destipinfoTimeZone,destipinfoAreaCode,destipinfoGlobalRoaming,destipinfoInternalCode,destipinfoStateCode,destipinfoOperator,destipinfoLongitude,destipinfoLatitude,destipinfoArea,ds");
        configMap.put("emptyFileds", "");
        configMap.put("ip", "172.31.134.216");
        configMap.put("port", "22");
        configMap.put("user", "docker");
        configMap.put("password", "docker");
        SftpSink sftpSink = new SftpSink(configMap);

//        AtdEventModel atdEventModel = new AtdEventModel();
//        atdEventModel.setTimestamp(new Date());
//        atdEventModel.setEventTypeId("--666--");

        DpiModel dpiModel = new DpiModel();
        dpiModel.setDestIp("0.0.0.0");
        dpiModel.setDestPort("8080");

        sftpSink.invoke(Arrays.asList(dpiModel),null);
    }
}
