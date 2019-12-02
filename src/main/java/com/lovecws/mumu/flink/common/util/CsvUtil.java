package com.lovecws.mumu.flink.common.util;

import au.com.bytecode.opencsv.CSVWriter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.net.URLEncoder;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @program: act-able
 * @description: csv文件工具类，读取csv文件、写入csv文件
 * @author: 甘亮
 * @create: 2019-06-18 18:52
 **/
public class CsvUtil {

    private static final Logger log = Logger.getLogger(CsvUtil.class);

    public static List<Object> getFieldValues(List<Map<String, String>> columnFields, Map<String, Object> cloumnValueMap) {
        List<Object> valueList = new ArrayList<>();
        columnFields.forEach(columnFieldMap -> {
            String columnFieldName = columnFieldMap.get("name");
            String columnFieldType = columnFieldMap.get("type");
            Object mapValue = MapFieldUtil.getMapField(cloumnValueMap, columnFieldName, "");
            if ("timestamp".equalsIgnoreCase(columnFieldType) && mapValue instanceof Date) {
                mapValue = new Timestamp(((Date) mapValue).getTime());
            }
            //解決字符串包含換行
            if (mapValue instanceof String) {
                        /*if (String.valueOf(mapValue).contains("\n") || String.valueOf(mapValue).contains(sperator)) {
                            mapValue = URLEncoder.encode(String.valueOf(mapValue));
                        }*/
                mapValue = EscapeUtil.escape(mapValue.toString());
            }
            valueList.add(mapValue);
        });
        return valueList;
    }

    public static void writeFile(List<Object> datas, String filePath, String sperator) {
        if (datas == null || datas.size() == 0) return;

        List<Map<String, String>> tableFields = TableUtil.getTableFields(datas.get(0));
        BufferedWriter bufferedWriter = null;
        try {
            bufferedWriter = new BufferedWriter(new FileWriter(new File(filePath), true));
            for (Object data : datas) {
                List<Object> valueList = new ArrayList<>();
                Map<String, Object> cloumnValueMap = TableUtil.getCloumnValues(data, tableFields);
                tableFields.forEach(columnFieldMap -> {
                    String columnFieldName = columnFieldMap.get("name");
                    String columnFieldType = columnFieldMap.get("type");
                    Object mapValue = MapFieldUtil.getMapField(cloumnValueMap, columnFieldName, "");
                    if ("timestamp".equalsIgnoreCase(columnFieldType) && mapValue instanceof Date) {
                        mapValue = new Timestamp(((Date) mapValue).getTime());
                    }
                    //解決字符串包含換行
                    if (mapValue instanceof String) {
                        if (String.valueOf(mapValue).contains("\n") || String.valueOf(mapValue).contains(sperator)) {
                            mapValue = URLEncoder.encode(String.valueOf(mapValue));
                        }
                    }
                    valueList.add(mapValue);
                });
                bufferedWriter.append(StringUtils.join(valueList, sperator));
                bufferedWriter.newLine();
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        } finally {
            if (bufferedWriter != null) IOUtils.closeQuietly(bufferedWriter);
        }
    }

    @Deprecated
    public static void writeCSVFile(List<Object> datas, String filePath, char separator, boolean header) {
        if (datas == null || datas.size() == 0) return;

        List<Map<String, String>> tableFields = TableUtil.getTableFields(datas.get(0));
        try {
            CSVWriter csvWriter = new CSVWriter(new FileWriter(new File(filePath), true), separator);
            boolean headerInit = false;
            List<String> columnNameList = new ArrayList<>();
            for (Object data : datas) {
                List<String> valueList = new ArrayList<>();
                Map<String, Object> cloumnValueMap = TableUtil.getCloumnValues(data, tableFields);
                boolean finalHeaderInit = headerInit;
                tableFields.forEach(columnFieldMap -> {
                    String columnFieldName = columnFieldMap.get("name");
                    if (header && !finalHeaderInit) {
                        columnNameList.add(columnFieldName);
                    }
                    Object mapValue = MapFieldUtil.getMapField(cloumnValueMap, columnFieldName, "");
                    valueList.add(mapValue.toString());
                });
                if (header && !finalHeaderInit) {
                    csvWriter.writeNext(StringUtils.join(columnNameList, ",").split(","));
                }
                csvWriter.writeNext(StringUtils.join(valueList, ",").split(","));
                headerInit = true;
            }
            csvWriter.close();
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        }
    }

}
