package com.lovecws.mumu.flink.common.util;

import com.lovecws.mumu.flink.common.annotation.EsField;
import com.lovecws.mumu.flink.common.annotation.TableField;
import com.lovecws.mumu.flink.common.model.atd.MAtdEventModel;
import com.lovecws.mumu.flink.common.model.attack.FlowEventModel;
import com.lovecws.mumu.flink.common.model.gynetres.MinistryLoopholeModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @program: act-able
 * @description: tableUtil工具类测试
 * @author: 甘亮
 * @create: 2019-06-19 10:08
 **/
public class TableUtilTest {

    @Test
    public void getHiveColumn() {
        List<String> columns = new ArrayList<>();
        List<Map<String, String>> tableFields = TableUtil.getTableFields(new FlowEventModel());
        for (int i = 0; i < tableFields.size(); i++) {
            Map<String, String> columnMap = tableFields.get(i);
            String name = columnMap.get("name");
            columns.add(name);

            String type = columnMap.get("type");
            if (StringUtils.isEmpty(type)) type = "string";
            String comment = columnMap.get("comment");
            if (comment == null) comment = "";

            String[] strings = {String.valueOf(i + 1), name, type, comment};
            System.out.println(StringUtils.join(strings, "\t"));
        }
        System.out.println(StringUtils.join(columns, ","));
    }

    @Test
    public void getPgColumn() {
        List<Map<String, String>> tableFields = TableUtil.getTableFields(new MinistryLoopholeModel());
        int current_index = 0;
        for (int i = 0; i < tableFields.size(); i++) {
            Map<String, String> columnMap = tableFields.get(i);
            String name = columnMap.get("name");
            if ("serialVersionUID".equals(name)) continue;
            current_index++;
            String type = columnMap.get("type");
            if (StringUtils.isEmpty(type)) type = "text";
            String comment = columnMap.get("comment");
            if (comment == null) comment = "";

            String[] strings = {String.valueOf(current_index), name, type, comment};
            System.out.println(StringUtils.join(strings, "\t"));
        }
    }


    @Test
    public void getJdbcColumn() {
        int currentIndex = 0;
        Field[] allFields = FieldUtils.getFieldsWithAnnotation(MinistryLoopholeModel.class, com.baomidou.mybatisplus.annotation.TableField.class);
        for (int i = 0; i < allFields.length; i++) {
            Field field = allFields[i];
            com.baomidou.mybatisplus.annotation.TableField tableField = field.getAnnotation(com.baomidou.mybatisplus.annotation.TableField.class);
            if (tableField == null) continue;
            currentIndex++;
            String name = tableField.value();
            String type = "text";
            String comment = "";

            String[] strings = {String.valueOf(currentIndex), name, type, comment};
            System.out.println(StringUtils.join(strings, "\t"));
        }
    }

    @Test
    public void getEsColumn() {
        Field[] fields = FieldReverseUtils.getFieldsWithAnnotation(MAtdEventModel.class, EsField.class);
        if (fields == null || fields.length == 0) {
            fields = FieldReverseUtils.getAllFields(MinistryLoopholeModel.class);
        }
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            EsField esField = field.getAnnotation(EsField.class);

            String name = "";
            String type = "";
            String analyze = "";
            String format = "";
            String comment = "";
            if (esField != null) {
                name = esField.name();
                type = esField.type();
                analyze = esField.analyze();
                format = esField.format();
            } else {
                name = field.getName();
                type = "keyword";
                analyze = "not_analyzed";
                format = "";
            }

            TableField tableField = field.getAnnotation(TableField.class);
            if (tableField != null) {
                comment = tableField.comment();
            }
            String[] strings = {String.valueOf(i + 1), name, type, format, analyze, comment};
            System.out.println(StringUtils.join(strings, "\t"));

        }
    }
}
