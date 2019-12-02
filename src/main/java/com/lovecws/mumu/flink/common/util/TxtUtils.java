package com.lovecws.mumu.flink.common.util;

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class TxtUtils {

    protected static Logger log = LoggerFactory.getLogger(TxtUtils.class);

    /**
     * 全量字符串
     *
     * @param list
     * @param fields
     * @return
     */
    public static String list2txt(List<?> list, String[] fields) {
        // 获取实体类的所有属性信息，返回Field数组
        StringBuilder sb = new StringBuilder();
        if (list != null && list.size() > 0) {
            for (Object obj : list) {
                // 获取实体类的所有属性信息，返回Field数组
                String readLine = readLine(obj, fields);
                String line = System.getProperty("line.separator");
                sb.append(readLine).append(line);
            }
        }
        return sb.toString();
    }

    /**
     * 获取字符串list
     *
     * @param list
     * @param fields
     * @return
     */
    public static List<String> list2StrList(List<?> list, String[] fields) {

        List<String> listLine = new ArrayList<>();
        // 获取实体类的所有属性信息，返回Field数组
        if (list != null && list.size() > 0) {
            for (Object obj : list) {
                // 获取实体类的所有属性信息，返回Field数组
                String readLine = readLine(obj, fields);
                listLine.add(readLine);
            }
        }
        return listLine;
    }

    /**
     * 读取一行
     */
    public static String readLine(Object obj, String[] fields) {
        StringBuilder line = new StringBuilder();
        Field[] declaredFields = obj.getClass().getDeclaredFields();
        for (String field : fields) {
            for (Field fd : declaredFields) {
                fd.setAccessible(true);
                if (field.equals(fd.getName())) {
                    try {
                        //日期格式数据处理
                        Object o = fd.get(obj);
                        if (o == null) o = "";
                        if (o instanceof Timestamp) {
                            Calendar calendar = Calendar.getInstance();
                            calendar.setTimeInMillis(((Timestamp) o).getTime());
                            o = DateUtils.formatDate(calendar.getTime(), "yyyy-MM-dd HH:mm:ss");
                        } else if (o instanceof Date) {
                            o = DateUtils.formatDate((Date) o, "yyyy-MM-dd HH:mm:ss");
                        }
                        String value = String.valueOf(o);
                        //String value = fd.get(obj) == null ? "" : String.valueOf(fd.get(obj));
                        //按工业互联网部省接口规范处理字符串
                        value = value.replaceAll("[\\n\\r]", "").replaceAll("\\,", "%2C")
                                .replaceAll("\\\"", "%22").replaceAll("\\'", "%27")
                                .replaceAll("\\|", "%7C");
                        line.append(value).append("|");
                        break;
                    } catch (Exception e) {
                        log.error("转换错误", e);
                    }
                }
            }
        }
        return line.toString().substring(0, line.toString().length() - 1);
    }

    public static String getLine(Field fd, Object obj, String[] uploadFields) {
        try {
            //日期格式数据处理
            Object o = fd.get(obj);
            boolean isUpload = false;
            //如果上报字段为空 则上报所有的字段
            if (uploadFields == null || uploadFields.length == 0 || StringUtils.isBlank(uploadFields[0])) {
                isUpload = true;
            }
            //如果该字段属于上报字段 那么上报该字段的值
            else {
                for (String uploadField : uploadFields) {
                    if (fd.getName().equals(uploadField)) {
                        isUpload = true;
                        break;
                    }
                }
            }
            if (isUpload) {
                if (o == null) o = "";
                if (o instanceof Timestamp) {
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTimeInMillis(((Timestamp) o).getTime());
                    o = DateUtils.formatDate(calendar.getTime(), "yyyy-MM-dd HH:mm:ss");
                } else if (o instanceof Date) {
                    o = DateUtils.formatDate((Date) o, "yyyy-MM-dd HH:mm:ss");
                }
                String value = String.valueOf(o);
                //String value = fd.get(obj) == null ? "" : String.valueOf(fd.get(obj));
                //按工业互联网部省接口规范处理字符串
                value = value.replaceAll("[\\n\\r]", "").replaceAll("\\,", "%2C")
                        .replaceAll("\\\"", "%22").replaceAll("\\'", "%27")
                        .replaceAll("\\|", "%7C");
                return value;
            }
        } catch (Exception e) {
            log.error("转换错误", e);
        }
        return "";
    }

    public static String readLine(Object obj, String[] fields, String[] uploadFields) {
        StringBuilder line = new StringBuilder();
        Field[] declaredFields = obj.getClass().getDeclaredFields();
        //如果fields为空 则获取所有的字段
        if (fields == null || fields.length == 0 || StringUtils.isBlank(fields[0])) {
            for (Field fd : declaredFields) {
                fd.setAccessible(true);
                line.append(getLine(fd, obj, uploadFields)).append("|");
            }
        } else {
            for (String field : fields) {
                for (Field fd : declaredFields) {
                    fd.setAccessible(true);
                    if (field.equals(fd.getName())) {
                        line.append(getLine(fd, obj, uploadFields)).append("|");
                        break;
                    }
                }
            }
        }
        if (line.toString().length() > 0) {
            return line.toString().substring(0, line.toString().length() - 1);
        }
        return line.toString();
    }

    public static void transforModel(String line, String[] fields, Object obj) {
        transforModel(line, fields, obj, "EEE MMM dd HH:mm:ss zzz yyyy", "\\|");
    }

    public static void transforModel(String line, String[] fields, Object obj, String splitFormat) {
        transforModel(line, fields, obj, "EEE MMM dd HH:mm:ss zzz yyyy", splitFormat);
    }

    public static void transforModel(String line, String[] fields, Object obj, String simpleDateFormat, String splitFormat) {
        SimpleDateFormat sdf = new SimpleDateFormat(simpleDateFormat, Locale.US);
//		String[] values = line.split("\\|");
        String[] values = line.split(splitFormat);
        Field[] declaredFields = obj.getClass().getDeclaredFields();
        int length = values.length;
        for (int index = 0; index < length; index++) {
            String field = fields[index];
            String value = values[index];
            if (StringUtils.isEmpty(values[index]) || values[index].equalsIgnoreCase("null")) {
                continue;
            }
            value = value.toString().replaceAll("%2C", "\\,")
                    .replaceAll("%22", "\\\"").replaceAll("%27", "\\'")
                    .replaceAll("%7C", "\\|");
            if (StringUtils.isEmpty(value)) {
                continue;
            }
            for (Field fd : declaredFields) {
                fd.setAccessible(true);
                if (field.equals(fd.getName())) {
                    try {
                        if (fd.getType().equals(Integer.class)) {
                            fd.set(obj, Integer.valueOf(value));
                        } else if (fd.getType().equals(String.class)) {
                            fd.set(obj, String.valueOf(value));
                        } else if (fd.getType().equals(Long.class)) {
                            fd.set(obj, Long.valueOf(value));
                        } else if (fd.getType().equals(Date.class)) {
                            Date d = sdf.parse(values[index]);
                            fd.set(obj, d);
                        } else if (fd.getType().equals(Boolean.class)) {
                            fd.set(obj, Boolean.valueOf(value));
                        }
                        break;
                    } catch (Exception e) {
                        log.error(e.getLocalizedMessage() + "[" + line + "]", e);
                    }
                }
            }
        }
    }

    public static void transforModel(String line, String[] fields, Field[] declaredFields, Object obj, String simpleDateFormat, String splitFormat) {
        SimpleDateFormat sdf = new SimpleDateFormat(simpleDateFormat, Locale.US);
        String[] values = line.split(splitFormat);
        int length = values.length;
        for (int index = 0; index < length; index++) {
            String field = fields[index];
            String value = values[index];
            if (StringUtils.isEmpty(values[index]) || values[index].equalsIgnoreCase("null")) {
                continue;
            }
            value = value.replaceAll("%2C", "\\,")
                    .replaceAll("%22", "\\\"").replaceAll("%27", "\\'")
                    .replaceAll("%7C", "\\|");
            if (StringUtils.isEmpty(value)) {
                continue;
            }
            for (Field fd : declaredFields) {
                fd.setAccessible(true);
                if (field.equals(fd.getName())) {
                    try {
                        if (fd.getType().equals(Integer.class)) {
                            fd.set(obj, Integer.valueOf(value));
                        } else if (fd.getType().equals(String.class)) {
                            fd.set(obj, String.valueOf(value));
                        } else if (fd.getType().equals(Long.class)) {
                            fd.set(obj, Long.valueOf(value));
                        } else if (fd.getType().equals(Date.class)) {
                            Date d = sdf.parse(values[index]);
                            fd.set(obj, d);
                        } else if (fd.getType().equals(Boolean.class)) {
                            fd.set(obj, Boolean.valueOf(value));
                        }
                        break;
                    } catch (Exception e) {
                        log.error(e.getLocalizedMessage() + "[" + line + "]", e);
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws FileNotFoundException {
        AtomicLong counter = new AtomicLong();
        String path = "E:\\project\\workspace2019\\act-able\\code\\act-able\\trunk\\act-able-ads-modules\\act-industry-data\\act-industry-data-processing\\src";
        path = "E:\\project\\workspace2019\\act-able\\code\\act-able\\trunk\\act-able-ads-modules\\act-industry-data";
//        path = "E:\\project\\act-telecom\\gynet\\TD1701001_ads\\ads\\able-trunk\\springcloud_able\\act-able-ads-modules";
        Collection<File> files = FileUtils.listFiles(new File(path), new String[]{"java"}, true);
        for (File file : files) {

            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            ImmutableList<String> filterLines = ImmutableList.of("package", "import");

            AtomicBoolean hasComment = new AtomicBoolean(false);
            AtomicLong lineCounter = new AtomicLong(0);
            bufferedReader.lines().forEach(line -> {
                boolean flag = true;
                line = line.trim();
                //过滤空行
                if (StringUtils.isEmpty(line)) flag = false;
                //过滤首字母为非代码行数
                for (String filterWord : filterLines) {
                    if (line.contains(filterWord)) {
                        flag = false;
                        break;
                    }
                }
                //过滤单行注释 /** **/
                if ((line.startsWith("/**") && line.endsWith("**/")) || line.startsWith("//")) {
                    flag = false;
                }
                //过滤多行注释
                if (line.startsWith("/**") && !line.endsWith("**/")) {
                    flag = false;
                    hasComment.set(true);
                }
                //行注释结尾
                if (line.endsWith("**/")) {
                    flag = false;
                    hasComment.set(false);
                }
                if (flag && !hasComment.get()) lineCounter.addAndGet(1);

            });
            counter.addAndGet(lineCounter.get());
            log.info(file.getAbsolutePath() + " : " + lineCounter.get());

        }
        log.info(String.valueOf(counter.get()));
    }
}
