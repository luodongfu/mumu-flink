package com.lovecws.mumu.flink.common.util;

import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @program: act-able
 * @description: FileUtils测试类
 * @author: 甘亮
 * @create: 2019-06-20 17:21
 **/
public class FileUtilsTest {

    @Test
    public void mergeFiles() {
        String dir = "E:\\data\\dataprocessing\\storage\\gynetres\\ds=2019-07-18";
        Collection<File> files = org.apache.commons.io.FileUtils.listFiles(new File(dir), new String[]{"parquet"}, true);
        String mergeFile = FileUtils.mergeFiles(new ArrayList<>(files), dir, "SNAPPY", 500000);
        System.out.println(mergeFile);
    }

    @Test
    public void mergeJsonFiles() {
        String dir = "E:\\data\\dataprocessing\\storage\\flowjson\\20190724";
        Collection<File> files = org.apache.commons.io.FileUtils.listFiles(new File(dir), new String[]{"json"}, true);
        String mergeFile = FileUtils.mergeFiles(new ArrayList<>(files), dir, "SNAPPY", 500000);
        System.out.println(mergeFile);
    }
}
