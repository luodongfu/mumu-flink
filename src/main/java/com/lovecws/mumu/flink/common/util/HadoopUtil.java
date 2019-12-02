package com.lovecws.mumu.flink.common.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Logger;

import java.net.URI;

/**
 * @program: act-able
 * @description: hadoop获取hdfs工具
 * @author: 甘亮
 * @create: 2019-11-25 09:28
 **/
public class HadoopUtil {

    private static final Logger log = Logger.getLogger(HadoopUtil.class);

    /**
     * 从hdfs中获取hdfs路径，可以配置多个(第一个是主namenode节点，其他为standby节点)
     *
     * @param hdfsPath hdfs路径
     * @return DistributedFileSystem
     */
    public static DistributedFileSystem distributedFileSystem(String hdfsPath) {
        DistributedFileSystem distributedFileSystem = new DistributedFileSystem();
        String[] hdfsPaths = hdfsPath.split(",");
        for (String hp : hdfsPaths) {
            try {
                distributedFileSystem = new DistributedFileSystem();
                distributedFileSystem.initialize(new URI(hp), new Configuration());
                if (!distributedFileSystem.isInSafeMode()) return distributedFileSystem;
            } catch (Exception ex) {
                log.error(ex.getLocalizedMessage());
            }
        }
        return distributedFileSystem;
    }
}
