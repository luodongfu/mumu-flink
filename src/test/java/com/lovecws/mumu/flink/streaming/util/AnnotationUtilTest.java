package com.lovecws.mumu.flink.streaming.util;

import com.lovecws.mumu.flink.common.util.AnnotationUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @program: mumu-flink
 * @description: 注解工具类测试
 * @author: 甘亮
 * @create: 2019-07-18 19:54
 **/
public class AnnotationUtilTest {

    @Test
    public void getModularAnnotation() {
        Class modularAnnotation = AnnotationUtil.getModularAnnotation("source", "kafka");
        System.out.println(modularAnnotation.getName());
    }

    @Test
    public void putIfAbsent(){
        ConcurrentHashMap<String,Boolean> map=new ConcurrentHashMap<>();
        Boolean name = map.putIfAbsent("name", true);
        System.out.println(name);
        System.out.println(map);
        name = map.putIfAbsent("name", true);
        System.out.println(name);
    }

    public static void main2(String[] args) {
        Calendar calendar=Calendar.getInstance();
        calendar.setTimeInMillis(1548284400000L);
        calendar.setTimeInMillis(1545494400000L);
        calendar.setTimeInMillis(1543507200000L);
        calendar.setTimeInMillis(1546066297000L);
        System.out.println(calendar.getTime().toLocaleString());
    }

    public static void main(String[] args) {
        String[] fields="waId,eventTypeId,timestamp,sensorName,sensorIp,eventSource,uuid,rid,classtype,classtypeCn,category,categoryCn,subCategory,srcIp,srcPort,srcService,dstIp,dstPort,dstService,proto,appProto,killChain,killChainCn,payload,respData,severity,reliability,family,target,atdDesc,message,srcIpCountry,srcIpProvince,srcIpCity,srcIpCounty,srcIspName,srcIpGeoloc,srcIpCountryCode,standby15,dstIpCountry,dstIpProvince,dstIpCity,dstIpCounty,dstIspName,dstIpGeoloc,dstIpCountryCode,standby16,connSrcMac,connDstMac,connOrigBytes,connRespBytes,connOrigPkts,connRespPkts,connConnState,connDuration,httpMethod,httpHost,httpUri,httpReferrer,httpVersion,httpUserAgent,httpContentType,httpContentLength,httpStatusCode,dnsQuery,dnsQtypeName,dnsTtl,dnsAnswers,sumstatNumber,sumstatDuration,sumstatSample,standby5,standby6,standby7,standby8,standby9,ftpFtpCmd,smtpSmtpFrom,smtpSmtpTo,standby13,standby14,filePackage,fileSha256,fileSsdeep,fileFilename,fileTag,fileMd5,standby19,standby20,waCreateTime,id,corpId,corpName,industry,srcCorpId,srcCorpName,srcIndustry,beginTime,stopTime,srcIpValue,dstIpValue,streamMonitorInterface,severityCn,standby1,standby2,standby3,standby4,standby17,standby18,createTime,count,beginTimeStr,stopTimeStr,srcIpCountryFromIpNet,srcIpCountryCodeFromIpNet,srcIpProvinceFromIpNet,srcIpProvinceCodeFromIpNet,srcIpCityFromIpNet,srcIpCityCodeFromIpNet,dstIpCountryFromIpNet,dstIpCountryCodeFromIpNet,dstIpProvinceFromIpNet,dstIpProvinceCodeFromIpNet,dstIpCityFromIpNet,dstIpCityCodeFromIpNet,srcIpType,dstIpType,ipPortJoint,srcIpPort,ipJoint,dstIpPort,attackIp,attackPort,attackCountry,attackProvince,attackCity,attackCorpName,attackIndustry,attackGeoloc,attackedIp,attackedPort,attackedCountry,attackedProvince,attackedCity,attackedCorpName,attackedIndustry,attackedGeoloc,attackCorpType,attackedCorpType,uploadProvinceCode,uploadProvinceName,dataSource".split(",");
        String[] emptyFields="waId,sensorName,sensorIp,eventSource,uuid,rid,classtype,category,srcIp,srcService,dstService,killChain,payload,respData,reliability,family,target,atdDesc,message,srcIpCounty,srcIspName,srcIpGeoloc,srcIpCountryCode,standby15,dstIpCounty,dstIspName,dstIpGeoloc,dstIpCountryCode,standby16,connSrcMac,connDstMac,connOrigBytes,connRespBytes,connOrigPkts,connRespPkts,connConnState,connDuration,httpMethod,httpHost,httpUri,httpReferrer,httpVersion,httpUserAgent,httpContentType,httpContentLength,httpStatusCode,dnsQuery,dnsQtypeName,dnsTtl,dnsAnswers,sumstatNumber,sumstatDuration,sumstatSample,standby5,standby6,standby7,standby8,standby9,ftpFtpCmd,smtpSmtpFrom,smtpSmtpTo,standby13,standby14,filePackage,fileSha256,fileSsdeep,fileFilename,fileTag,fileMd5,standby19,standby20,waCreateTime,id,corpId,srcCorpId,beginTime,stopTime,srcIpValue,dstIpValue,streamMonitorInterface,standby1,standby2,standby3,standby4,standby17,standby18,count,beginTimeStr,stopTimeStr,srcIpCountryFromIpNet,srcIpCountryCodeFromIpNet,srcIpProvinceFromIpNet,srcIpProvinceCodeFromIpNet,srcIpCityFromIpNet,srcIpCityCodeFromIpNet,dstIpCountryFromIpNet,dstIpCountryCodeFromIpNet,dstIpProvinceFromIpNet,dstIpProvinceCodeFromIpNet,dstIpCityFromIpNet,dstIpCityCodeFromIpNet,srcIpType,dstIpType,ipPortJoint,srcIpPort,ipJoint,dstIpPort,attackIp,attackPort,attackCountry,attackProvince,attackCity,attackCorpName,attackIndustry,attackGeoloc,attackedIp,attackedPort,attackedCountry,attackedProvince,attackedCity,attackedCorpName,attackedIndustry,attackedGeoloc,attackCorpType,attackedCorpType".split(",");
        List<String> uploadFields = new ArrayList<>();
        for (String fd : fields) {
            boolean isUpload = true;
            for (String ef : emptyFields) {
                if (ef.equalsIgnoreCase(fd)) {
                    isUpload = false;
                    break;
                }
            }
            if (isUpload) uploadFields.add(fd);
        }
        System.out.println(StringUtils.join(uploadFields,","));
    }
}
