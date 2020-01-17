package com.lovecws.mumu.flink.common.jdbc;

import com.lovecws.mumu.flink.common.util.DateUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.Date;
import java.util.*;

/**
 * @program: mumu-flink
 * @description: pg存储，读取模型的TableField注解信息
 * @author: 甘亮
 * @create: 2019-05-24 17:42
 **/
@Slf4j
public class PostgreJdbcService extends BasicJdbcService {

    public PostgreJdbcService(JdbcConfig jdbcConfig) {
        super(jdbcConfig);
    }

    private static final String DEFAULT_PG_TYPE = "text";

    @Override
    public String getCreateTableSql(String primaryTable, Map<String, Object> tableInfo) {
        List<Map<String, String>> columns = (List<Map<String, String>>) tableInfo.get("columns");
        StringBuilder columnBuilder = new StringBuilder();
        columns.forEach(columnMap -> {
            if (columnMap != null) {
                String type = columnMap.get("type");
                if (StringUtils.isEmpty(type)) {
                    type = DEFAULT_PG_TYPE;
                }
                columnBuilder.append(columnMap.get("name") + " " + type + ",");
            }
        });

        if (columnBuilder.toString().endsWith(",")) columnBuilder.deleteCharAt(columnBuilder.length() - 1);
        StringBuffer createTableBuffer = new StringBuffer("CREATE TABLE IF NOT EXISTS " + primaryTable + " ( " + columnBuilder.toString() + ")");

        //获取索引信息和分区信息
        List<Map<String, Object>> tableIndexs = (List<Map<String, Object>>) tableInfo.get("indexing");
        Map<String, Object> partitionMap = (Map<String, Object>) tableInfo.get("partition");

        List<Map<String, Object>> indexMaps = new ArrayList<>();
        List<String> tables = new ArrayList<>();
        //分区注解
        if (partitionMap != null && partitionMap.size() > 0) {

            String partitionType = partitionMap.get("type").toString();
            String interval = partitionMap.get("interval").toString();
            String partitionFields = partitionMap.get("fields").toString();
            int rangePartitionCount = Integer.parseInt(partitionMap.getOrDefault("count", "7").toString());
            Map<String, Object> partitionTable = (Map<String, Object>) partitionMap.get("table");

            createTableBuffer.append(" PARTITION BY " + partitionType + "(" + partitionFields + ");\n\n");
            if (partitionTable != null && partitionTable.size() > 0) {
                for (Map.Entry<String, Object> entry : partitionTable.entrySet()) {
                    String tableName = entry.getValue().toString();
                    if ("list".equalsIgnoreCase(partitionType)) {
                        createTableBuffer.append("CREATE TABLE IF NOT EXISTS " + primaryTable + "_" + tableName + " PARTITION OF " + primaryTable + " FOR VALUES IN (" + entry.getKey() + ");\n");
                        tables.add(tableName);
                    } else if ("range".equalsIgnoreCase(partitionType)) {
                        for (int i = 0; i < rangePartitionCount; i++) {
                            Calendar calendar = Calendar.getInstance();
                            calendar.setTime(jdbcConfig.getCurrentDate());
                            if ("day".equalsIgnoreCase(interval)) {
                                calendar.add(Calendar.DAY_OF_MONTH, i);
                            } else if ("week".equalsIgnoreCase(interval)) {
                                calendar.add(Calendar.WEEK_OF_MONTH, i);
                            } else if ("month".equalsIgnoreCase(interval)) {
                                calendar.add(Calendar.DAY_OF_MONTH, i);
                            } else if ("year".equalsIgnoreCase(interval)) {
                                calendar.add(Calendar.YEAR, i);
                            } else if ("hour".equalsIgnoreCase(interval)) {
                                calendar.add(Calendar.HOUR_OF_DAY, i);
                            }

                            String tableDate = DateUtils.getPartitionTable(calendar.getTime(), interval);
                            //本周周一时间
                            String startDay = DateUtils.getPartitionTableBegining(calendar.getTime(), interval);
                            //本周周六时间
                            String endDay = DateUtils.getPartitionTableEnding(calendar.getTime(), interval);

                            String partitionFrom = entry.getKey() + "," + startDay;
                            String partitionTo = entry.getKey() + "," + endDay;
                            createTableBuffer.append("CREATE TABLE IF NOT EXISTS " + primaryTable + "_" + tableName + "_" + tableDate + " PARTITION OF " + primaryTable + " FOR VALUES FROM (" + partitionFrom + ") TO (" + partitionTo + ");\n");
                            tables.add(primaryTable + "_" + tableName + "_" + tableDate);
                        }
                    }
                    createTableBuffer.append("\n");
                }
            }
        } else {
            tables.add(primaryTable);
        }
        createTableBuffer.append("\n\n");
        //添加表索引
        if (tableIndexs != null && tableIndexs.size() > 0) {
            for (Map<String, Object> tableIndex : tableIndexs) {
                for (String tableName : tables) {
                    Map<String, Object> indexMap = new HashMap<>(tableIndex);
                    indexMap.put("table", tableName);
                    indexMaps.add(indexMap);
                }
            }
        }
        createTableBuffer.append("\n");
        //添加索引
        for (Map<String, Object> indexMap : indexMaps) {
            String indexType = indexMap.get("type").toString();
            String indexTable = indexMap.get("table").toString();
            String indexFields = indexMap.get("fields").toString();
            String indexSQL = "";
            if ("pk".equalsIgnoreCase(indexType)) {
                indexSQL = "ALTER TABLE " + indexTable + " DROP CONSTRAINT IF EXISTS " + indexTable + "_pk;\n";
                indexSQL = indexSQL + "ALTER TABLE " + indexTable + " ADD CONSTRAINT " + indexTable + "_pk PRIMARY KEY(" + indexFields + ");\n";
            } else if ("index".equalsIgnoreCase(indexType)) {
                indexSQL = "CREATE INDEX IF NOT EXISTS " + indexTable + "_index ON " + indexTable + "(" + indexFields + ");\n";
            }
            createTableBuffer.append(indexSQL);
        }
        return createTableBuffer.toString();
    }

    /**
     * 获取当前(本日、本周、本月、本年)分区表
     *
     * @param primaryTable 主表
     * @param tableInfo    字段信息
     * @return
     */
    public List<String> getCurrentPartitionTables(String primaryTable, Map<String, Object> tableInfo) {
        List<String> tables = new ArrayList<>();
        Map<String, Object> partitionMap = (Map<String, Object>) tableInfo.get("partition");
        if (partitionMap != null && partitionMap.size() > 0) {

            String partitionType = partitionMap.get("type").toString();
            String interval = partitionMap.get("interval").toString();
            Map<String, Object> partitionTable = (Map<String, Object>) partitionMap.get("table");

            if (partitionTable != null && partitionTable.size() > 0) {
                for (Map.Entry<String, Object> entry : partitionTable.entrySet()) {
                    String tableName = entry.getValue().toString();
                    if ("list".equalsIgnoreCase(partitionType)) {
                        tables.add(tableName);
                    } else if ("range".equalsIgnoreCase(partitionType)) {
                        String tableDate = DateUtils.getPartitionTable(jdbcConfig.getCurrentDate(), interval);
                        tables.add(primaryTable + "_" + tableName + "_" + tableDate);
                    }
                }
            }
        }
        return tables;
    }

    @Override
    public boolean batchUpset(String table, Map<String, Object> tableInfoMap, List<Map<String, Object>> eventDatas) {
        List<Map<String, String>> columns = (List<Map<String, String>>) tableInfoMap.get("columns");
        Map partitionMap = MapUtils.getMap(tableInfoMap, "partition", new HashMap());

        String fields = MapUtils.getString(partitionMap, "fields", "");
        String interval = MapUtils.getString(partitionMap, "interval", "day");
        Map tableMap = MapUtils.getMap(partitionMap, "table", new HashMap());

        Connection connection = null;
        Statement statement = null;
        List<String> sqls = new ArrayList<>();
        try {
            connection = jdbcConfig.getConnection();
            connection.setAutoCommit(false);
            statement = connection.createStatement();

            String ds = DateUtils.getPartitionTable(jdbcConfig.getCurrentDate(), interval);
            String dateStr = DateUtils.formatDate(jdbcConfig.getCurrentDate(), "yyyyMMddHHmmss");
            for (Map<String, Object> eventDataMap : eventDatas) {
                String partitionTable = table;
                if (StringUtils.isNotEmpty(fields)) {
                    for (String partitionField : fields.split(",")) {
                        Object pv = eventDataMap.get(partitionField);
                        if (pv == null || "".equals(pv)) continue;
                        Object object = tableMap.get(pv);
                        if (object == null) {
                            object = ds;
                            eventDataMap.put(partitionField, dateStr);
                        }
                        partitionTable = partitionTable + "_" + object.toString();
                    }
                }
                String upsetStatement = upsetStatement(partitionTable, columns, eventDataMap, Arrays.asList("begin_time"), Arrays.asList("count"));
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
                if (value instanceof String) {
                    String escapaStr = ((String) value).replaceAll("'", "").replaceAll("\"", "");
                    valueBuffer.append("\'" + escapaStr + "\'");
                } else if (value instanceof Date) {
                    valueBuffer.append("\'" + new Timestamp(((Date) value).getTime()) + "\'");
                } else {
                    valueBuffer.append(value);
                }
                if (i < columns.size() - 1) {
                    columnBuffer.append(",");
                    valueBuffer.append(",");
                }

                boolean partition = Boolean.parseBoolean(columnMap.getOrDefault("partition", "false"));
                boolean indexing = Boolean.parseBoolean(columnMap.getOrDefault("indexing", "false"));
                if (!partition && !indexing && !excludeUpdateColumns.contains(columnName)) {
                    if (counterUpdateColumns.contains(columnName)) {
                        conflictBuffer.append(columnName + " = " + table + "." + columnName + "+excluded." + columnName + ",");
                    } else {
                        conflictBuffer.append(columnName + " = excluded." + columnName + ",");
                    }
                }
            }
            if (conflictBuffer.toString().endsWith(",")) conflictBuffer.deleteCharAt(conflictBuffer.length() - 1);
            String bulkSql = "INSERT INTO " + table + " (" + columnBuffer.toString() + ") values (" + valueBuffer.toString() + ") ON conflict(id) DO UPDATE SET " + conflictBuffer.toString();
            log.debug(bulkSql);
            return bulkSql;
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        }
        return null;
    }

    @Override
    public boolean upset(String table, List<Map<String, String>> columns, Map<String, Object> eventData) {
        StringBuilder columnBuffer = new StringBuilder();
        StringBuilder dotaBuffer = new StringBuilder();
        StringBuilder conflictBuffer = new StringBuilder();

        List<Object> valueList = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            Map<String, String> columnMap = columns.get(i);
            String columnName = columnMap.get("name");
            columnBuffer.append(columnName);
            valueList.add(eventData.get(columnName));

            dotaBuffer.append("?");
            if (i < columns.size() - 1) {
                columnBuffer.append(",");
                dotaBuffer.append(",");
            }

            boolean partition = Boolean.parseBoolean(columnMap.getOrDefault("partition", "false"));
            boolean indexing = Boolean.parseBoolean(columnMap.getOrDefault("indexing", "false"));
            if (!partition && !indexing && !columnName.equalsIgnoreCase("begin_time")) {
                if (columnName.equalsIgnoreCase("count")) {
                    conflictBuffer.append(columnName + " = " + table + ".count+excluded.count,");
                } else {
                    conflictBuffer.append(columnName + " = excluded." + columnName + ",");
                }
            }
        }
        if (conflictBuffer.toString().endsWith(",")) conflictBuffer.deleteCharAt(conflictBuffer.length() - 1);
        String bulkSql = "INSERT INTO " + table + " (" + columnBuffer.toString() + ") values (" + dotaBuffer.toString() + ") ON conflict(id) DO UPDATE SET " + conflictBuffer.toString();
        log.debug(bulkSql);
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = jdbcConfig.getConnection();
            preparedStatement = connection.prepareStatement(bulkSql);
            for (int i = 0; i < valueList.size(); i++) {
                int currentIndex = i + 1;
                Object value = valueList.get(0);
                if (value == null) {
                    preparedStatement.setObject(currentIndex, value);
                } else {
                    if (value instanceof Date) {
                        preparedStatement.setTimestamp(currentIndex, new Timestamp(((Date) value).getTime()));
                    } else if (value instanceof Integer) {
                        preparedStatement.setInt(currentIndex, Integer.parseInt(value.toString()));
                    } else if (value instanceof Long) {
                        preparedStatement.setLong(currentIndex, Long.parseLong(value.toString()));
                    } else {
                        preparedStatement.setObject(currentIndex, value);
                    }
                }
            }
            return preparedStatement.execute();
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage() + "[" + bulkSql + "]", ex);
        } finally {
            try {
                if (preparedStatement != null) preparedStatement.close();
                if (connection != null) connection.close();
            } catch (Exception e) {
                log.error(e.getLocalizedMessage(), e);
            }
        }
        return false;
    }

    /**
     * 获取pg数据库的表分区列表
     *
     * @param tableName 主表
     * @return
     */
    public List<String> getPgPartitionTables(String tableName) {
        Connection connection = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        String partitionSql = "SELECT nmsp_parent.nspname AS parent_schema,parent.relname AS parent,nmsp_child.nspname AS child,child.relname AS child_schema " +
                "FROM pg_inherits JOIN pg_class parent ON pg_inherits.inhparent = parent.oid JOIN pg_class child ON pg_inherits.inhrelid = child.oid JOIN pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace JOIN pg_namespace nmsp_child ON nmsp_child.oid = child.relnamespace " +
                "WHERE parent.relname = '" + tableName + "';";
        List<String> partitionTables = new ArrayList<>();
        try {
            connection = jdbcConfig.getConnection();
            preparedStatement = connection.prepareStatement(partitionSql);

            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String child_schema = resultSet.getString("child_schema");
                partitionTables.add(child_schema);
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

    @Override
    public boolean databaseExists(String databaseName) {
        Connection connection = jdbcConfig.getConnection();
        ResultSet rs = null;
        try {
            Statement statement = connection.createStatement();
            rs = statement.executeQuery("SELECT u.datname  FROM pg_catalog.pg_database u where u.datname='" + databaseName + "'");
            return rs.next();
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
            try {
                if (rs != null) rs.close();
                if (connection != null) connection.close();
            } catch (Exception e) {
                log.error(e.getLocalizedMessage(), e);
            }
        }
        return false;
    }

    @Override
    public boolean createDatabase(String databaseName) {
        Connection connection = jdbcConfig.getConnection();
        Statement statement = null;
        try {
            statement = connection.createStatement();
            return statement.execute("create database " + databaseName);
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
            try {
                if (statement != null) statement.close();
                if (connection != null) connection.close();
            } catch (Exception e) {
                log.error(e.getLocalizedMessage(), e);
            }
        }
        return false;
    }
}
