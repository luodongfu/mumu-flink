package com.lovecws.mumu.flink.streaming.common.constants;

import java.io.Serializable;

/**
 * @Author: lyc
 * @Date: 2019-6-26 16:21
 * @Description: ${DESCRIPTION}
 */
public class DpiConstant implements Serializable {
    public static String ASSET_FEATURES_CACHE = "industry_dpiDataFormatService_assetFeatures_cache_type";
    public static String CORPNAME_INDUSTRY_CACHE = "industry_dpiDataFormatService_corpnameIndustry_cache";

    public static String DPI_FIELD_PREFIX = "dpi";

    public static String DPI_ENCRY_SPECIAL_CHARACTERS = "|||";
    public static String DPI_DECRY_SPECIAL_CHARACTERS = "\\|\\|\\|";

    public static String DPI_FIELD_DEVICEINFO = "deviceinfo";
    public static String DPI_FIELD_DEVICEINFO_PRIMARY_NAMECN = "primary_namecn";
    public static String DPI_FIELD_DEVICEINFO_SECONDARY_NAMECN = "secondary_namecn";
    public static String DPI_FIELD_DEVICEINFO_VENDOR = "vendor";
    public static String DPI_FIELD_DEVICEINFO_SERVICE_DESC = "service_desc";
    public static String DPI_FIELD_DEVICEINFO_CLOUD_PLATFORM = "cloud_Platform";
    public static String DPI_FIELD_DEVICEINFO_DEVICEID = "deviceid";
    public static String DPI_FIELD_DEVICEINFO_SN = "sn";
    public static String DPI_FIELD_DEVICEINFO_MAC = "mac";
    public static String DPI_FIELD_DEVICEINFO_MD5= "md5";
    public static String DPI_FIELD_DEVICEINFO_OS = "os";
    public static String DPI_FIELD_DEVICEINFO_SOFTVERSION = "softVersion";
    public static String DPI_FIELD_DEVICEINFO_FIRMWAREVERSION = "firmwareVersion";
    public static String DPI_FIELD_DEVICEINFO_IP = "ip";
    public static String DPI_FIELD_DEVICEINFO_NUMBER = "number";
}
