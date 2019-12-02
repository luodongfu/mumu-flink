package com.lovecws.mumu.flink.common.jdbc;

import com.lovecws.mumu.flink.common.util.AvroUtil;
import com.lovecws.mumu.flink.common.util.HadoopUtil;
import com.lovecws.mumu.flink.common.util.MapFieldUtil;
import com.lovecws.mumu.flink.common.util.ParquetUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @program: act-able
 * @description: hive存储，读取模型的TableField注解信息
 * @author: 甘亮
 * @create: 2019-05-24 17:42
 **/
@Slf4j
public class HiveJdbcService extends BasicJdbcService {

    public HiveJdbcService(JdbcConfig jdbcConfig) {
        super(jdbcConfig);
    }

    private static final String DEFAULT_HIVE_TYPE = "string";

    /**
     * 获取到hive创建表ddl语句
     *
     * @param table     表名
     * @param tableInfo 表字段信息
     * @return
     */
    @Override
    public String getCreateTableSql(String table, Map<String, Object> tableInfo) {
        List<Map<String, String>> columns = (List<Map<String, String>>) tableInfo.get("columns");
        String createTableSql = "";
        StringBuilder columnBuilder = new StringBuilder();
        StringBuilder partitionBuffer = new StringBuilder();
        columns.forEach(columnMap -> {
            String type = columnMap.get("type");
            String name = columnMap.get("name");
            String comment = columnMap.get("comment");
            if (comment == null) {
                comment = "";
            }
            /*try {
                comment = new String(comment.getBytes("UTF-8"), "ISO-8859-1");
            } catch (Exception ex) {
            }*/

            if (StringUtils.isEmpty(type)) {
                type = DEFAULT_HIVE_TYPE;
            }
            boolean partition = Boolean.parseBoolean(columnMap.getOrDefault("partition", "false"));
            if (partition) {
                partitionBuffer.append(name + " " + type + " comment '" + comment + "'" + ",");
            } else {
                columnBuilder.append(name + " " + type + " comment '" + comment + "'" + ",");
            }
        });

        if (columnBuilder.toString().endsWith(",")) {
            columnBuilder.deleteCharAt(columnBuilder.length() - 1);
        }
        if (partitionBuffer.toString().endsWith(",")) {
            partitionBuffer.deleteCharAt(partitionBuffer.length() - 1);
        }
        createTableSql = "CREATE TABLE IF NOT EXISTS " + table + " ( " + columnBuilder.toString() + ")";
        if (partitionBuffer.length() > 0) {
            createTableSql = createTableSql + " PARTITIONED BY (" + partitionBuffer.toString() + ")";
        }
        //表存储格式
        Map tableProperties = MapUtils.getMap(tableInfo, "properties", new HashMap());
        //序列化反序列化
        Object serde = tableProperties.get("serde");
        if (serde != null && !"".equalsIgnoreCase(serde.toString())) {
            createTableSql += " ROW FORMAT SERDE '" + serde.toString() + "'";
        }
        //数据存储格式
        Object storage = tableProperties.get("storage");
        if (storage != null && !"".equalsIgnoreCase(storage.toString())) {
            createTableSql += " STORED AS " + storage.toString().toUpperCase() + "";
        }
        //自定义存储格式
        else {
            Object inputFormat = tableProperties.get("inputFormat");
            Object outputFormat = tableProperties.get("outputFormat");
            if (inputFormat != null && !"".equalsIgnoreCase(inputFormat.toString()) && outputFormat != null && !"".equalsIgnoreCase(outputFormat.toString())) {
                createTableSql += " STORED AS INPUTFORMAT '" + inputFormat.toString() + "' OUTPUTFORMAT '" + outputFormat.toString() + "'";
            }
        }
        //数据存储路径
        Object location = tableProperties.get("location");
        if (location != null && !"".equalsIgnoreCase(location.toString())) {
            createTableSql += " LOCATION '" + location.toString() + "'";
        }
        Object properties = tableProperties.get("properties");
        if (properties != null && !"".equalsIgnoreCase(properties.toString())) {
            createTableSql += " TBLPROPERTIES (" + properties.toString() + ")";
        }
        return createTableSql;
    }

    /**
     * hive使用 preparedStatement插入数据 但是preparedStatement不支持批量插入
     *
     * @param table        表
     * @param columns      字段列表
     * @param eventDataMap
     * @return
     */
    @Override
    public boolean insertInto(String table, List<Map<String, String>> columns, Map<String, Object> eventDataMap) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            //获取分区字段
            List<String> partitionFields = new ArrayList<>();
            List<String> columnFields = new ArrayList<>();

            columns.forEach(columnMap -> {
                String columnName = columnMap.get("name");
                if (Boolean.parseBoolean(columnMap.getOrDefault("partition", "false"))) {
                    partitionFields.add(columnName);
                } else {
                    columnFields.add(columnName);
                }
            });
            StringBuffer partitionBuffer = new StringBuffer();
            List<Object> valueList = new ArrayList<>();
            List<Object> fieldList = new ArrayList<>();
            List<String> dotaFields = new ArrayList<>();

            //分区字段不能为空
            partitionFields.forEach(partitionField -> {
                partitionBuffer.append(partitionField + "=\"" + eventDataMap.getOrDefault(partitionField, "").toString() + "\",");
            });

            for (String columnField : columnFields) {
                Object value = eventDataMap.get(columnField);
                //过滤空值数据
                if (value == null) {
                    continue;
                }
                if (value instanceof Date) {
                    valueList.add(new Timestamp(((Date) value).getTime()));
                } else if (value instanceof String) {
                    //字符编码问题  如果hive数据库配置的是拉丁默认编码 这里需要转码
                    if ("UTF-8".equalsIgnoreCase(jdbcConfig.getCharset())) {
                        valueList.add(value);
                    } else {
                        valueList.add(new String(value.toString().getBytes("UTF-8"), jdbcConfig.getCharset()));
                    }
                } else {
                    valueList.add(value);
                }
                fieldList.add(columnField);
                dotaFields.add("?");
            }

            String partition = partitionBuffer.toString();
            if (partition.endsWith(",")) {
                partition = partition.substring(0, partition.length() - 1);
            }

            if (partition.length() > 0) {
                partition = "partition (" + partition + ")";
            }
            String insertSQL = "INSERT INTO " + table + " " + partition + " (" + StringUtils.join(fieldList, ",") + ") values(" + StringUtils.join(dotaFields, ",") + ")";
            log.info(insertSQL);

            connection = jdbcConfig.getConnection();
            preparedStatement = connection.prepareStatement(insertSQL);
            for (int i = 1; i <= valueList.size(); i++) {
                preparedStatement.setObject(i, valueList.get(i - 1));
            }
            return preparedStatement.execute();
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
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
     * 使用statement 批量执行数据插入，但是数据格式会存在问题（有的数据包含换行、html编码等特殊符号），目前使用urlEncode将数据进行编码
     *
     * @param table      表
     * @param columns    字段列表
     * @param eventDatas 数据集
     * @return
     */
    @Override
    public boolean batchInsertInto(String table, List<Map<String, String>> columns, List<Map<String, Object>> eventDatas) {

        Connection connection = null;
        Statement statement = null;
        try {
            connection = jdbcConfig.getConnection();
            statement = connection.createStatement();

            //获取分区字段
            List<String> partitionFields = new ArrayList<>();
            List<String> columnFields = new ArrayList<>();
            columns.forEach(columnMap -> {
                String columnName = columnMap.get("name");
                if (Boolean.parseBoolean(columnMap.getOrDefault("partition", "false"))) {
                    partitionFields.add(columnName);
                } else {
                    columnFields.add(columnName);
                }
            });
            //获取每个分区的集合
            Map<String, List<String>> eventKeyValuesMap = new HashMap<>();
            eventDatas.forEach(eventDataMap -> {
                StringBuffer keyBuffer = new StringBuffer();
                partitionFields.forEach(partitionField -> {
                    keyBuffer.append(partitionField + "=\"" + eventDataMap.getOrDefault(partitionField, "").toString() + "\",");
                });
                if (keyBuffer.toString().endsWith(",")) {
                    keyBuffer.deleteCharAt(keyBuffer.length() - 1);
                }
                StringBuffer valueBuffer = new StringBuffer();
                columnFields.forEach(columnField -> {
                    Object value = eventDataMap.get(columnField);
                    if (value instanceof String) {
                        try {
                            String cvalue = null;
                            if ("UTF-8".equalsIgnoreCase(jdbcConfig.getCharset())) {
                                cvalue = value.toString();
                            } else {
                                cvalue = new String(value.toString().getBytes("UTF-8"), jdbcConfig.getCharset());
                            }
                            //cvalue = EscapeUtil.escape(cvalue);
                            cvalue = cvalue
//                                    .replaceAll(",", "%2C")
//                                    .replaceAll(":", "%3A")
                                    .replaceAll("\"", "%22")
                                    .replaceAll("'", "%27")
                                    .replaceAll("\n", "%0A")
                                    .replaceAll("\r", "%0D");
                            //如果字符创 包含 “ 则对字段值进行urlencode编码
                            if (cvalue.contains("\"")) {
                                cvalue = URLEncoder.encode(cvalue);
                            }
                            valueBuffer.append("\"" + cvalue + "\"");
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                    } else if (value instanceof Date) {
                        //valueBuffer.append("\"" + DateUtils.formatDate((Date) value, "yyyy-MM-dd HH:mm:ss") + "\"");
                        valueBuffer.append("\"" + new Timestamp(((Date) value).getTime()) + "\"");
                    } else {
                        valueBuffer.append(value);
                    }
                    valueBuffer.append(",");
                });
                if (valueBuffer.toString().endsWith(",")) {
                    valueBuffer.deleteCharAt(valueBuffer.length() - 1);
                }

                List<String> values = eventKeyValuesMap.get(keyBuffer.toString());
                if (values == null) {
                    values = new ArrayList<>();
                }
                values.add("(" + valueBuffer.toString() + ")");
                eventKeyValuesMap.put(keyBuffer.toString(), values);
            });
            for (Map.Entry<String, List<String>> entry : eventKeyValuesMap.entrySet()) {
                String partition = "";
                if (entry.getKey().length() > 0) {
                    partition = "partition (" + entry.getKey() + ")";
                }
                String insertSQL = "INSERT INTO " + table + " " + partition + " (" + StringUtils.join(columnFields, ",") + ") values" + StringUtils.join(entry.getValue(), ",");
                log.debug(insertSQL);
                statement.execute(insertSQL);
            }
            log.info("executeBatch success " + table + " count:" + eventDatas.size());
            return true;
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage());
        } finally {
            try {
                if (statement != null) statement.close();
                if (connection != null) connection.close();
            } catch (Exception e) {
                log.error(e.getLocalizedMessage(), e);
            }
        }
        return false;
    }

    private Map<String, Map<String, Object>> hiveFileMap = new HashMap<>();

    /**
     * hive将数据保存在本地文件
     *
     * @param baseDir      表名
     * @param columns      字段集合
     * @param eventDataMap 数据集合
     * @return
     */
    public boolean insertIntoLocalFile(List<Map<String, String>> columns, Map<String, Object> eventDataMap, String baseDir, String storage, String compressionCodec, int timeCount, int lineCount) {
        return batchInsertIntoLocalFile(columns, Arrays.asList(eventDataMap), baseDir, storage, compressionCodec, timeCount, lineCount);
    }

    /**
     * hive将数据保存在本地文件
     *
     * @param columns    字段集合
     * @param eventDatas 数据集合
     * @param baseDir    写入文件的路径
     * @param storage    数据存储方式(csv、parquet、avro、sequencefile)
     * @param timeCount  文件超时事件
     * @param lineCount  文件限定数量
     * @return
     */
    public boolean batchInsertIntoLocalFile(List<Map<String, String>> columns, List<Map<String, Object>> eventDatas, String baseDir, String storage, String compressionCodec, int timeCount, int lineCount) {
        if (baseDir != null) {
            baseDir = baseDir.replaceAll("\\\\", "/");
            if (baseDir.endsWith("/")) {
                baseDir = baseDir.substring(0, baseDir.length() - 1);
            }
        }
        //获取分区字段信息 和分区字段值
        Map<String, List<Map<String, Object>>> partitionMap = new HashMap<>();
        try {
            List<String> partitionFields = new ArrayList<>();
            List<Map<String, String>> columnFields = new ArrayList<>();
            columns.forEach(columnMap -> {
                String columnName = columnMap.get("name");
                if (Boolean.parseBoolean(columnMap.getOrDefault("partition", "false"))) {
                    partitionFields.add(columnName);
                } else {
                    columnFields.add(columnMap);
                }
            });
            eventDatas.forEach(eventDataMap -> {
                StringBuffer partitionPathBuffer = new StringBuffer();
                partitionFields.forEach(partitionField -> partitionPathBuffer.append("/" + partitionField + "=" + eventDataMap.get(partitionField)));
                List<Map<String, Object>> maps = partitionMap.get(partitionPathBuffer.toString());
                if (maps == null || maps.size() == 0) {
                    maps = new ArrayList<>();
                }
                maps.add(eventDataMap);
                partitionMap.put(partitionPathBuffer.toString(), maps);
            });
            //将分区数据分别写入到不同的文件中
            String finalBaseDir = baseDir;
            partitionMap.entrySet().forEach(entry -> {
                String key = entry.getKey();
                List<Map<String, Object>> values = entry.getValue();
                insertIntoLocalFile(key, values, columnFields, finalBaseDir, storage, compressionCodec, timeCount, lineCount);
            });
            return true;
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        }
        return false;
    }

    /**
     * 将数据写入到本地文件
     *
     * @param partitionPath 分区路径
     * @param values        字段值集合
     * @param columnFields  字段列表
     * @param baseDir       写入文件的路径
     * @param storage       数据存储方式(csv、parquet、avro、sequencefile)
     * @param timeCount     文件超时事件
     * @param lineCount     文件限定数量
     */
    private void insertIntoLocalFile(String partitionPath, List<Map<String, Object>> values, List<Map<String, String>> columnFields, String baseDir, String storage, String compressionCodec, int timeCount, int lineCount) {
        //如果数据为空 则不写入文件 提防空文件生成
        if (values == null || values.size() == 0) return;
        BufferedWriter bufferedWriter = null;
        String currentFile = "";
        int count = 1;
        try {
            String newFileName = UUID.randomUUID().toString().replace("-", "");
            Map<String, Object> fileMap = hiveFileMap.get(partitionPath);
            if (fileMap == null) {
                fileMap = new HashMap<>();
                fileMap.put("createTime", System.currentTimeMillis());
                currentFile = newFileName;
            } else {
                currentFile = fileMap.get("file").toString();
                int fileCount = Integer.parseInt(fileMap.get("count").toString());
                //当当前事件-文件创建事件 > timeCount*1000毫秒 之后重新生成新的文件
                if (timeCount > 0 && (System.currentTimeMillis() - Long.parseLong(fileMap.get("createTime").toString()) > timeCount * 1000)) {
                    currentFile = newFileName;
                }
                if (lineCount > 0 && fileCount > lineCount) {
                    currentFile = newFileName;
                }
                //如果没有启用事件分区创建文件和行数创建文件 则每一次都创建一个文件
                if (timeCount == -1 && lineCount == -1) {
                    currentFile = newFileName;
                }
                if (timeCount > 0 || lineCount > 0) {
                    count = fileCount + 1;
                }
            }

            //创建目录
            FileUtils.forceMkdir(new File(baseDir + partitionPath));

            String filePath = baseDir + partitionPath + "/" + currentFile;
            if (storage != null) {
                filePath = filePath + "." + storage.toLowerCase();
            }
            //parquet存储方式
            if ("parquet".equalsIgnoreCase(storage)) {
                ParquetUtil.writeFile(Arrays.asList(values.toArray()), ParquetUtil.getMessageType(columnFields), filePath, compressionCodec);
            }
            //avro存储方式
            else if ("avro".equalsIgnoreCase(storage)) {
                AvroUtil.writeFile(values, columnFields, filePath, compressionCodec);
            }
            //textfile文件存储
            else {
                bufferedWriter = new BufferedWriter(new FileWriter(new File(filePath), true));
                for (Map<String, Object> map : values) {
                    List<Object> valueList = new ArrayList<>();
                    columnFields.forEach(columnFieldMap -> {
                        //解决hive日期数据加载问题
                        String columnFieldName = columnFieldMap.get("name");
                        String columnFieldType = columnFieldMap.get("type");
                        Object mapValue = MapFieldUtil.getMapField(map, columnFieldName, "");
                        if ("timestamp".equalsIgnoreCase(columnFieldType) && mapValue instanceof Date) {
                            mapValue = new Timestamp(((Date) mapValue).getTime());
                        }
                        //解決字符串包含特殊字符，对这些字符进行转义
                        if (mapValue instanceof String) {
                           /* if (String.valueOf(mapValue).contains("\n")) {
                                mapValue = URLEncoder.encode(String.valueOf(mapValue));
                            }*/
                            //mapValue = EscapeUtil.escape(mapValue.toString());
                            mapValue = mapValue.toString()
//                                    .replaceAll(":", "%3A")
//                                    .replaceAll(",", "%2C")
                                    .replaceAll("\"", "%22")
                                    .replaceAll("'", "%27")
                                    .replaceAll("\n", "%0A")
                                    .replaceAll("\r", "%0D");
                        }
                        valueList.add(mapValue);
                    });
                    bufferedWriter.append(StringUtils.join(valueList, "\u0001"));
                    bufferedWriter.newLine();
                }
            }

            fileMap.put("count", count);
            fileMap.put("file", currentFile);
            hiveFileMap.put(partitionPath, fileMap);
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        } finally {
            try {
                if (bufferedWriter != null) {
                    bufferedWriter.close();
                }
            } catch (Exception ex) {
                log.error(ex.getLocalizedMessage(), ex);
            }
        }
    }

    /**
     * 批量插入数据(hive首先将文件写入到hdfs中,然后将文件加载到hive中)
     * load data inpath local '/tmp/score.txt' into table score partition (school="school1",class="class1")
     *
     * @param table            表
     * @param dataPath         本地文件路径
     * @param hdfsPath         hdfs路徑
     * @param compressionCodec 存儲压缩格式
     * @param timeCount        timeCount
     * @param lineCount        文件合并最大行数
     * @return
     */
    public void loadDataLocalInpath(String table, String dataPath, String hdfsPath, String compressionCodec, int timeCount, int lineCount) {
        File dataPathDir = new File(dataPath);
        if (!dataPathDir.exists()) {
            return;
        }
        if (StringUtils.isNotEmpty(hdfsPath) && !hdfsPath.endsWith("/")) {
            hdfsPath = hdfsPath + "/";
        }

        //文件目录按照分区收集
        Map<String, List<File>> partitionFileMap = new HashMap<>();
        Collection<File> dataPaths = FileUtils.listFiles(dataPathDir, null, true);
        final AtomicInteger fileCounter = new AtomicInteger();
        dataPaths.forEach(dataPathFile -> {
            //删除隐藏文件
            if (dataPathFile.isHidden() || FilenameUtils.isExtension(dataPathFile.getName(), "crc")) {
                FileUtils.deleteQuietly(dataPathFile);
            } else {
                //当前时间超过文件最后修改时间
                if ((System.currentTimeMillis() - dataPathFile.lastModified()) > timeCount * 1000) {
                    String partitionPath = dataPathFile.getParent().substring(dataPath.length()).replace("\\", "/");
                    if (partitionPath.startsWith("/")) {
                        partitionPath = partitionPath.substring(1);
                    }
                    List<File> files = partitionFileMap.get(partitionPath);
                    if (files == null) {
                        files = new ArrayList<>();
                    }
                    files.add(dataPathFile);
                    partitionFileMap.put(partitionPath, files);
                    fileCounter.addAndGet(1);
                }
            }
        });
        log.info("scan dataPath:" + dataPath + " fileCount:" + fileCounter.get());
        if (fileCounter.get() == 0) {
            return;
        }

        //将事件数据写入到本地文件中
        Connection connection = null;
        Statement statement = null;
        DistributedFileSystem distributedFileSystem = null;

        Map<String, String> loadDataInpathMap = new HashMap<>();
        Map<String, String> mergeFileMap = new HashMap<>();
        List<String> mergeFiles = new ArrayList<>();
        try {
            for (Map.Entry<String, List<File>> entry : partitionFileMap.entrySet()) {
                String partitionPath = entry.getKey();
                StringBuffer partitionBuffer = new StringBuffer();
                for (String key : partitionPath.split("/")) {
                    String[] nameValue = key.split("=");
                    if (nameValue.length == 2) {
                        partitionBuffer.append(nameValue[0] + "=" + "'" + nameValue[1] + "',");
                    }
                }
                if (partitionBuffer.toString().endsWith(",")) {
                    partitionBuffer.deleteCharAt(partitionBuffer.length() - 1);
                }

                //合并文件 将一个分区的文件合并成一个文件
                String mergeBasePath = dataPath;
                if (!mergeBasePath.endsWith("/")) {
                    mergeBasePath = mergeBasePath + "/";
                }
                mergeBasePath = mergeBasePath + partitionPath;
                String mergeFilePath = com.lovecws.mumu.flink.common.util.FileUtils.mergeFiles(entry.getValue(), mergeBasePath, compressionCodec, lineCount);
                //合并文件的時候出現合并文件為空的情況，所以过滤这类文件
                if (mergeFilePath == null) continue;
                mergeFiles.add(mergeFilePath);

                String finalMergeFilePath = mergeFilePath;
                //是否需要上传到hdfs上
                if (StringUtils.isNotEmpty(hdfsPath)) {
                    distributedFileSystem = HadoopUtil.distributedFileSystem(hdfsPath);
                    distributedFileSystem.mkdirs(new Path(hdfsPath + entry.getKey()));
                    String hdfsFilePath = hdfsPath + entry.getKey() + "/" + FilenameUtils.getName(finalMergeFilePath);
                    long l = FileUtils.sizeOf(new File(finalMergeFilePath));
                    if (l == 0L) {
                        continue;
                    }
                    distributedFileSystem.copyFromLocalFile(new Path(finalMergeFilePath), new Path(hdfsFilePath));
                    finalMergeFilePath = hdfsFilePath;
                }

                //文件真实路径
                loadDataInpathMap.put(finalMergeFilePath, partitionBuffer.toString());
                mergeFileMap.putIfAbsent(finalMergeFilePath, mergeFilePath);
            }

            //将[local|hdfs]文件load到hive上
            connection = jdbcConfig.getConnection();
            statement = connection.createStatement();
            for (Map.Entry<String, String> entry : loadDataInpathMap.entrySet()) {
                String local = "LOCAL";
                if (entry.getKey().toLowerCase().contains("hdfs")) {
                    local = "";
                }
                String partition = "";
                if (StringUtils.isNotEmpty(entry.getValue())) {
                    partition = " partition(" + entry.getValue() + ")";
                }
                String loadSQL = "LOAD DATA " + local + " INPATH '" + entry.getKey() + "' INTO TABLE " + table + partition;
                log.info(loadSQL);
                try {
                    statement.execute(loadSQL);
                    //执行成功 删除文件
                    String localMergeFilePath = mergeFileMap.get(entry.getKey());
                    FileUtils.deleteQuietly(new File(localMergeFilePath));
                } catch (Exception ex) {
                    log.error(ex.getLocalizedMessage() + "[" + loadSQL + "]", ex);
                    //load数据报错 则不删除该文件
                }
            }
            //删除已经收集的文件
            //mergeFiles.forEach(filePath -> FileUtils.deleteQuietly(new File(filePath)));
        } catch (Exception ex) {
            if (ex.getLocalizedMessage().contains("not supported in state standby")) {
                //TODO

            }
            log.error(ex.getLocalizedMessage(), ex);
        } finally {
            try {
                if (statement != null) statement.close();
                if (connection != null) connection.close();
                if (distributedFileSystem != null) distributedFileSystem.close();
            } catch (Exception e) {
                log.error(e.getLocalizedMessage(), e);
            }
        }
    }

    @Override
    public boolean databaseExists(String databaseName) {
        return false;
    }

    @Override
    public boolean createDatabase(String databaseName) {
        Connection connection = jdbcConfig.getConnection();
        Statement statement = null;
        try {
            statement = connection.createStatement();
            boolean execute = statement.execute("CREATE DATABASE IF NOT EXISTS " + databaseName + "");
            log.info("create database " + databaseName);
            return execute;
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage());
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
