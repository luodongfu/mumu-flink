package com.lovecws.mumu.flink.common.util;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @program: act-able
 * @description: 文件工具类
 * @author: 甘亮
 * @create: 2019-06-20 15:33
 **/
public class FileUtils {

    private static final Logger log = Logger.getLogger(FileUtils.class);
    /**
     * 合并文件
     *
     * @param files            文件集合
     * @param fileDir          文件目录
     * @param compressionCodec 压缩类型(当文件类型的时候指定压缩类型)
     * @param mergeLineCount  合并的最大行数
     * @return
     */
    public static String mergeFiles(List<File> files, String fileDir, String compressionCodec, int mergeLineCount) {
        //默认文件最大行数500000
        if (mergeLineCount <= 0) mergeLineCount = 1000000;
        if (files == null || files.size() == 0) return null;
        if (files.size() == 1) return files.get(0).getAbsolutePath();

        //目前只支持收集一定数据量的数据,防止文件量太大 导致生成太大的文件
        List<File> mergedFiles = new ArrayList<>();
        String fileType = FilenameUtils.getExtension(files.get(0).getName());
        if (!fileDir.endsWith("/")) fileDir = fileDir + "/";
        String mergeFile = fileDir + DateUtils.formatDate(new Date(), "yyyyMMddHHmmss") + "_" + UUID.randomUUID().toString().replace("-", "");
        if (StringUtils.isNotEmpty(fileType)) mergeFile = mergeFile + "." + fileType.toLowerCase();
        try {
            if ("parquet".equalsIgnoreCase(fileType)) {
                mergedFiles = ParquetUtil.mergeFiles(files, mergeFile, fileType, compressionCodec, mergeLineCount);
            } else if ("avro".equalsIgnoreCase(fileType)) {
                mergedFiles = AvroUtil.mergeFiles(files, mergeFile, fileType, compressionCodec, mergeLineCount);
            } else {
                AtomicLong counter = new AtomicLong(0);
                //文本文件
                BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(mergeFile));
                for (File file : files) {
                    if (counter.get() > mergeLineCount) break;
                    if (StringUtils.isEmpty(fileType) || file.getAbsolutePath().endsWith(fileType.toLowerCase())) {
                        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
                        bufferedReader.lines().forEach(line->{
                            counter.addAndGet(1);
                            try {
                                bufferedWriter.write(line);
                                bufferedWriter.newLine();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });
                        bufferedWriter.flush();

                        mergedFiles.add(file);
                        IOUtils.closeQuietly(bufferedReader);
                    }
                }
                IOUtils.closeQuietly(bufferedWriter);
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage(), ex);
        }
        //文件合并
        if (mergedFiles != null && mergedFiles.size() > 0) {
            //mergedFiles.forEach(File::deleteOnExit);
            mergedFiles.forEach(file -> org.apache.commons.io.FileUtils.deleteQuietly(file));
            return mergeFile;
        }
        return null;
    }
}
