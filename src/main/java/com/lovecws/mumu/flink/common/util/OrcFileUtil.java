package com.lovecws.mumu.flink.common.util;

import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * @program: mumu-flink
 * @description: 读取写入rcfile orcfile
 * @author: 甘亮
 * @create: 2019-06-21 17:11
 **/
public class OrcFileUtil {

    private static List<String> readRCFile(String rcFile) {
        Configuration configuration = new Configuration();

        List<String> list = new ArrayList<String>();
        try {

        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }
}
