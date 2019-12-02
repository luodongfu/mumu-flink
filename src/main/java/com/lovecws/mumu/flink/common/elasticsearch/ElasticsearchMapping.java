package com.lovecws.mumu.flink.common.elasticsearch;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 映射
 * @date 2018-06-03 16:59
 */
public class ElasticsearchMapping implements Serializable {

    public static final Logger log = Logger.getLogger(ElasticsearchMapping.class);

    /**
     * 创建es的mapping映射
     *
     * @param typeName 类型名称
     * @param mappings 映射关系
     * @return
     */
    public static XContentBuilder mapping(String typeName, List<MappingEntity> mappings) {
        try {
            XContentBuilder contentBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject(typeName)
                    .startObject("_source")
                    .field("enabled", true)
                    .endObject()
                    .startObject("_all")
                    .field("enabled", false)
                    .endObject()
                    .startObject("properties");
            for (MappingEntity mapping : mappings) {
                if (StringUtils.isNotEmpty(mapping.getFieldIndex())) {
                    contentBuilder
                            .startObject(mapping.getFieldName())
                            .field("type", mapping.getFieldType())
                            .field("index", mapping.getFieldIndex());
                } else {
                    contentBuilder
                            .startObject(mapping.getFieldName())
                            .field("type", mapping.getFieldType());
                }
                if (StringUtils.isNotEmpty(mapping.getFormat())) {
                    contentBuilder.field("format", mapping.getFormat());
                }
                contentBuilder.endObject();
            }
            contentBuilder.endObject().endObject().endObject();
            return contentBuilder;
        } catch (IOException e) {
            log.error(e);
            throw new IllegalArgumentException("mapping 映射错误");
        }
    }

    /**
     * 将map子弹值转化为XContentBuilder
     *
     * @param valueMap 值map
     * @return
     * @throws IOException
     */
    public static XContentBuilder content(Map<String, Object> valueMap) throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
        for (Map.Entry<String, Object> entry : valueMap.entrySet()) {
            xContentBuilder.field(entry.getKey(), entry.getValue());
        }
        xContentBuilder.endObject();
        return xContentBuilder;
    }

    public static class MappingEntity implements Serializable {

        private String fieldName;
        private String fieldType;
        private String fieldIndex;
        private String format;

        public MappingEntity(String fieldName, String fieldType, String fieldIndex) {
            this.fieldName = fieldName;
            this.fieldType = fieldType;
            this.fieldIndex = fieldIndex;
        }

        public MappingEntity(String fieldName, String fieldType, String fieldIndex, String format) {
            this.fieldName = fieldName;
            this.fieldType = fieldType;
            this.fieldIndex = fieldIndex;
            this.format = format;
        }

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getFieldType() {
            return fieldType;
        }

        public void setFieldType(String fieldType) {
            this.fieldType = fieldType;
        }

        public String getFieldIndex() {
            return fieldIndex;
        }

        public void setFieldIndex(String fieldIndex) {
            this.fieldIndex = fieldIndex;
        }

        public String getFormat() {
            return format;
        }

        public void setFormat(String format) {
            this.format = format;
        }

        @Override
        public String toString() {
            return "MappingEntity{" +
                    "fieldName='" + fieldName + '\'' +
                    ", fieldType='" + fieldType + '\'' +
                    ", fieldIndex='" + fieldIndex + '\'' +
                    ", format='" + format + '\'' +
                    '}';
        }
    }

}
