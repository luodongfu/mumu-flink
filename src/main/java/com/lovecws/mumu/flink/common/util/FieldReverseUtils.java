package com.lovecws.mumu.flink.common.util;

import org.apache.commons.lang3.Validate;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: trunk
 * @description: 获取字段集合
 * @author: 甘亮
 * @create: 2019-08-28 19:19
 **/
public class FieldReverseUtils {

    public static List<Field> getAllFieldsList(final Class<?> cls) {
        Validate.isTrue(cls != null, "The class must not be null");
        final List<List<Field>> allFields = new ArrayList<List<Field>>();
        Class<?> currentClass = cls;
        while (currentClass != null) {
            final List<Field> currentFields = new ArrayList<Field>();
            final Field[] declaredFields = currentClass.getDeclaredFields();
            for (final Field field : declaredFields) {
                currentFields.add(field);
            }
            allFields.add(0, currentFields);
            currentClass = currentClass.getSuperclass();
        }

        List<Field> fields = new ArrayList<>();
        for (List<Field> fs : allFields) {
            fs.forEach(field -> fields.add(field));
        }
        return fields;
    }

    public static Field[] getAllFields(final Class<?> cls) {
        List<Field> allFieldsList = getAllFieldsList(cls);
        return allFieldsList.toArray(new Field[allFieldsList.size()]);
    }

    public static List<Field> getFieldsListWithAnnotation(final Class<?> cls, final Class<? extends Annotation> annotationCls) {
        Validate.isTrue(annotationCls != null, "The annotation class must not be null");
        final List<Field> allFields = getAllFieldsList(cls);
        final List<Field> annotatedFields = new ArrayList<Field>();
        for (final Field field : allFields) {
            if (field.getAnnotation(annotationCls) != null) {
                annotatedFields.add(field);
            }
        }
        return annotatedFields;
    }

    public static Field[] getFieldsWithAnnotation(final Class<?> cls, final Class<? extends Annotation> annotationCls) {
        final List<Field> annotatedFieldsList = getFieldsListWithAnnotation(cls, annotationCls);
        return annotatedFieldsList.toArray(new Field[annotatedFieldsList.size()]);
    }
}
