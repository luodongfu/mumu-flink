package com.lovecws.mumu.flink.common.jdbc;

import com.lovecws.mumu.flink.common.util.DateUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.Date;
import java.util.*;

/**
 * @program: trunk
 * @description:  gbase数据库
 * @author: 甘亮
 * @create: 2019-10-16 08:57
 **/
@Slf4j
public class GbaseJdbcService extends BasicJdbcService {

    private static final String DEFAULT_GBASE_TYPE = "text";

    public GbaseJdbcService(JdbcConfig jdbcConfig) {
        super(jdbcConfig);
    }

    /*!50100 PARTITION BY RANGE (DAY(ds))
    SUBPARTITION BY HASH (event_type_id)
    SUBPARTITIONS 20
    (PARTITION malicious_software VALUES LESS THAN (20191016) ENGINE = GsDB,
     PARTITION loophole_scan VALUES LESS THAN (20191017) ENGINE = GsDB)
    */

    /**
     * @param primaryTable
     * @param tableInfo    表字段信息
     * @return
     */
    @Override
    public String getCreateTableSql(String primaryTable, Map<String, Object> tableInfo) {
        List<Map<String, String>> columns = (List<Map<String, String>>) tableInfo.get("columns");

        //分区
        Map<String, String> tablePartitionInfo = jdbcConfig.getTableInfo();

        //获取需要转换的类型 就是将一个类型转换为另外一个类型
        String fieldTypeConvert = tablePartitionInfo.get("fieldTypeConvert");
        final Map<String, String> typeMap = new HashMap<>();
        if (StringUtils.isNotEmpty(fieldTypeConvert)) {
            for (String typeField : fieldTypeConvert.split(",")) {
                String[] types = typeField.split("->");
                typeMap.put(types[0].toUpperCase(), types[1]);
            }
        }

        //按照字段进行类型转换
        String fieldNameConvert = tablePartitionInfo.get("fieldNameConvert");
        final Map<String, String> nameMap = new HashMap<>();
        if (StringUtils.isNotEmpty(fieldNameConvert)) {
            for (String nameField : fieldNameConvert.split(",")) {
                String[] nameType = nameField.split("->");
                nameMap.put(nameType[0].toUpperCase(), nameType[1]);
            }
        }

        StringBuilder columnBuilder = new StringBuilder();
        columns.forEach(columnMap -> {
            if (columnMap != null) {
                String type = columnMap.get("type");
                String name = columnMap.get("name");
                if (StringUtils.isEmpty(type)) {
                    type = DEFAULT_GBASE_TYPE;
                }
                if (typeMap.containsKey(type.toUpperCase())) type = typeMap.get(type.toUpperCase());
                if (nameMap.containsKey(name.toUpperCase())) type = nameMap.get(name.toUpperCase());
                columnBuilder.append(name + " " + type + ",");
            }
        });

        if (columnBuilder.toString().endsWith(",")) columnBuilder.deleteCharAt(columnBuilder.length() - 1);
        StringBuffer createTableBuffer = new StringBuffer("CREATE TABLE IF NOT EXISTS " + primaryTable + " ( " + columnBuilder.toString() + ") ");

        //创建分区表
        String partitionFieldObj = tablePartitionInfo.get("partitionField");
        String partitionTypeObj = tablePartitionInfo.get("partitionType");
        if (partitionFieldObj != null && partitionTypeObj != null) {
            StringBuffer partitionBuffer = new StringBuffer("/*!50100 ");
            StringBuffer partitionRangeBuffer = new StringBuffer();
            String[] partitionFields = partitionFieldObj.split(",");
            String[] partitionTypes = partitionTypeObj.split(",");
            if (partitionFields.length != partitionTypes.length) throw new IllegalArgumentException();
            for (int i = 0; i < partitionFields.length; i++) {
                String partitionField = partitionFields[i];
                String partitionType = partitionTypes[i];
                if ("range".equalsIgnoreCase(partitionType)) {
                    String partitionRangeInterval = tablePartitionInfo.getOrDefault("partitionRangeInterval", "day").toString();
                    String partitionRangeExpr = tablePartitionInfo.getOrDefault("partitionRangeExpr", "MICROSECOND").toString();

                    //bigint是将分区转换设置为空
                    if ("BIGINT".equalsIgnoreCase(partitionRangeExpr)) {
                        partitionBuffer.append(" PARTITION BY RANGE (" + partitionField + ")");
                    } else {
                        partitionBuffer.append(" PARTITION BY RANGE (" + partitionRangeExpr + "(" + partitionField + ")) ");
                    }

                    String partitionTableEnding = DateUtils.getPartitionTableEnding(new Date(), partitionRangeInterval);
                    Date date = DateUtils.stringToLongDate(partitionTableEnding);
                    Object partitionValue = partitionTableEnding;
                    if ("MICROSECOND".equalsIgnoreCase(partitionRangeExpr)) {
                        partitionValue = date.getTime();
                    }

                    String partitionTable = DateUtils.getPartitionTable(new Date(), partitionRangeInterval);
                    partitionRangeBuffer.append(" (PARTITION " + partitionField + "_" + partitionTable + " VALUES LESS THAN (" + partitionValue + "))");
                } else if ("hash".equalsIgnoreCase(partitionType)) {
                    int partitionHashCount = Integer.parseInt(tablePartitionInfo.getOrDefault("partitionHashCount", "10").toString());
                    partitionBuffer.append(" SUBPARTITION BY HASH (" + partitionField + ") SUBPARTITIONS " + partitionHashCount);
                }
            }
            partitionBuffer.append(partitionRangeBuffer.toString());
            partitionBuffer.append("*/");
            createTableBuffer.append(partitionBuffer.toString() + " ;");
        }
        //创建索引
        String indexFields = tablePartitionInfo.get("indexFields");
        String indexType = tablePartitionInfo.getOrDefault("indexType", "primarykey");

        StringBuilder indexBuilder = new StringBuilder();
        if (StringUtils.isNotEmpty(indexFields)) {
            if ("primarykey".equalsIgnoreCase(indexType)) {
                indexBuilder.append(" ALTER TABLE " + primaryTable + " ADD PRIMARY KEY (" + indexFields + ");");
            }
        }
        createTableBuffer.append(indexBuilder.toString());
        return createTableBuffer.toString();
    }

    /**
     * 获取当前需要创建的分区表
     *
     * @param table     表
     * @param tableInfo 表属性
     * @return
     */
    public String getCurrentPartitionTables(String table, Map<String, Object> tableInfo) {
        Map<String, String> tablePartitionInfo = jdbcConfig.getTableInfo();
        String partitionFieldObj = tablePartitionInfo.get("partitionField");
        String partitionTypeObj = tablePartitionInfo.get("partitionType");
        if (partitionFieldObj != null && partitionTypeObj != null) {
            String[] partitionFields = partitionFieldObj.split(",");
            String[] partitionTypes = partitionTypeObj.split(",");
            if (partitionFields.length != partitionTypes.length) throw new IllegalArgumentException();

            //range分区需要提交将分区表创建完成
            String partitionField = partitionFields[0];
            String partitionType = partitionTypes[0];
            if ("range".equalsIgnoreCase(partitionType)) {
                String partitionRangeInterval = tablePartitionInfo.getOrDefault("partitionRangeInterval", "day").toString();
                return partitionField + "_" + DateUtils.getPartitionTable(new Date(), partitionRangeInterval);
            }
        }
        return null;
    }

    /**
     * 获取分区表下的所有分区
     *
     * @param table
     * @return
     */
    public List<String> getGbasePartitionTables(String table) {
        Connection connection = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        String partitionSql = " select distinct partition_name from information_schema.partitions where table_name='" + table + "'";
        List<String> partitionTables = new ArrayList<>();
        try {
            connection = jdbcConfig.getConnection();
            preparedStatement = connection.prepareStatement(partitionSql);

            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String partition_name = resultSet.getString("partition_name");
                partitionTables.add(partition_name);
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage() + "[" + partitionSql + "]", ex);
        } finally {
            try {
                if (resultSet != null) resultSet.close();
                if (preparedStatement != null) preparedStatement.close();
                if (connection != null) connection.close();
            } catch (Exception e) {
                log.error(e.getLocalizedMessage(), e);
            }
        }
        return partitionTables;
    }

    /**
     * alter table abis_call_mr_partition add partition (partition ds_20191028 values less than (20191028));
     *
     * @param table
     * @param tableInfo
     * @return
     */
    public String getAddPartitionSql(String table, Map<String, Object> tableInfo) {
        Map<String, String> tablePartitionInfo = jdbcConfig.getTableInfo();
        String partitionRangeInterval = tablePartitionInfo.getOrDefault("partitionRangeInterval", "day");

        String partitionTableEnding = DateUtils.getPartitionTableEnding(new Date(), partitionRangeInterval);
        //获取分区表名称
        String currentPartitionTable = getCurrentPartitionTables(table, tableInfo);
        return "alter table " + table + " add partition (partition " + currentPartitionTable + " values less than (" + partitionTableEnding + "))";
    }


    @Override
    public boolean batchUpset(String table, Map<String, Object> tableInfoMap, List<Map<String, Object>> eventDatas) {
        List<Map<String, String>> columns = (List<Map<String, String>>) tableInfoMap.get("columns");

        Connection connection = null;
        Statement statement = null;
        List<String> sqls = new ArrayList<>();
        try {
            connection = jdbcConfig.getConnection();
            connection.setAutoCommit(false);
            statement = connection.createStatement();

            for (Map<String, Object> eventDataMap : eventDatas) {
                String upsetStatement = upsetStatement(table, columns, eventDataMap, Arrays.asList("begin_time"), Arrays.asList("count"));
                if (upsetStatement != null) {
                    sqls.add(upsetStatement);
                    statement.addBatch(upsetStatement);
                }
            }
            if (sqls.size() > 0) {
                statement.executeBatch();
            }
            connection.commit();
            connection.setAutoCommit(true);
            log.info("batchUpset table[" + table + "],count[" + sqls.size() + "]");
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        } finally {
            try {
                if (statement != null) statement.close();
                if (connection != null) connection.close();
            } catch (Exception e) {
                log.error(e.getLocalizedMessage(), e);
            }
        }
        return true;
    }

    public String upsetStatement(String table, List<Map<String, String>> columns, Map<String, Object> eventData, List<String> excludeUpdateColumns, List<String> counterUpdateColumns) {
        if (excludeUpdateColumns == null) excludeUpdateColumns = new ArrayList<>();
        if (counterUpdateColumns == null) counterUpdateColumns = new ArrayList<>();
        StringBuilder columnBuffer = new StringBuilder();
        StringBuilder valueBuffer = new StringBuilder();
        StringBuilder conflictBuffer = new StringBuilder();

        try {
            for (int i = 0; i < columns.size(); i++) {
                Map<String, String> columnMap = columns.get(i);
                String columnName = columnMap.get("name");
                Object value = eventData.get(columnName);
                columnBuffer.append(columnName);
                Object fieldValue = null;
                if (value instanceof String) {
                    String escapaStr = ((String) value).replaceAll("'", "").replaceAll("\"", "");
                    fieldValue = "\'" + escapaStr + "\'";
                } else if (value instanceof Date) {
                    fieldValue = "\'" + new Timestamp(((Date) value).getTime()) + "\'";
                } else {
                    fieldValue = value;
                }
                valueBuffer.append(fieldValue);

                if (i < columns.size() - 1) {
                    columnBuffer.append(",");
                    valueBuffer.append(",");
                }

                boolean partition = Boolean.parseBoolean(columnMap.getOrDefault("partition", "false"));
                boolean indexing = Boolean.parseBoolean(columnMap.getOrDefault("indexing", "false"));
                if (!partition && !indexing && !excludeUpdateColumns.contains(columnName) && value != null) {
                    if (counterUpdateColumns.contains(columnName)) {
                        conflictBuffer.append(columnName + " = " + columnName + "+" + fieldValue + ",");
                    } else {
                        conflictBuffer.append(columnName + " = " + fieldValue + ",");
                    }
                }
            }
            if (conflictBuffer.toString().endsWith(",")) conflictBuffer.deleteCharAt(conflictBuffer.length() - 1);
            String bulkSql = "INSERT INTO " + table + " (" + columnBuffer.toString() + ") values (" + valueBuffer.toString() + ") ON DUPLICATE KEY UPDATE " + conflictBuffer.toString();
            log.debug(bulkSql);
            return bulkSql;
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        }
        return null;
    }
}
