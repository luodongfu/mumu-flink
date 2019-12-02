package com.lovecws.mumu.flink.streaming.source;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.util.HadoopUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.util.List;
import java.util.Map;

/**
 * @program: act-able
 * @description: hdfs文件分析
 * @author: 甘亮
 * @create: 2019-11-22 14:58
 **/
@Slf4j
@ModularAnnotation(type = "source", name = "hdfs")
public class HdfsFileSource extends BaseFileSource {
    public HdfsFileSource(Map<String, Object> configMap) {
        super(configMap);
    }

    @Override
    public void collectFiles(String path, List<String> collectFiles, int batchCount) {
        DistributedFileSystem distributedFileSystem = HadoopUtil.distributedFileSystem(path);
        try {
            //获取目录下的所有hdfs文件
            RemoteIterator<LocatedFileStatus> listFiles = distributedFileSystem.listFiles(new Path(path), true);
            while (listFiles.hasNext()) {
                LocatedFileStatus fileStatus = listFiles.next();
                //防止读取到正在写入的文件
                if (System.currentTimeMillis() - fileStatus.getModificationTime() < 1000 * 60 * 2L) continue;

                String filePath = fileStatus.getPath().toUri().getPath();
                if (filterFile(filePath)) continue;

                collectFiles.add(filePath);
                if (batchCount > 0 && collectFiles.size() >= batchCount) return;
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage());
        } finally {
            IOUtils.closeQuietly(distributedFileSystem);
        }
    }

    @Override
    public void handleFile(SourceContext<Object> ctx, String filePath, byte[] bytes, String fileType) {
        DistributedFileSystem distributedFileSystem = HadoopUtil.distributedFileSystem(filePath);
        try {
            FSDataInputStream dataInputStream = distributedFileSystem.open(new Path(filePath));
            byte[] hdfsBytes = new byte[dataInputStream.available()];
            IOUtils.read(dataInputStream, hdfsBytes);
            IOUtils.closeQuietly(dataInputStream);

            super.handleFile(ctx, filePath, hdfsBytes, fileType);
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage());
        } finally {
            IOUtils.closeQuietly(distributedFileSystem);
        }
    }

    /**
     * 缓存文件信息
     *
     * @param filePath 文件路径
     */
    @Override
    public void cacheFile(String filePath) {
        if ("delete".equalsIgnoreCase(filterMode)) {
            DistributedFileSystem distributedFileSystem = null;
            try {
                distributedFileSystem = HadoopUtil.distributedFileSystem(filePath);
                distributedFileSystem.delete(new Path(filePath), false);
            } catch (Exception ex) {
                log.error(ex.getLocalizedMessage());
            } finally {
                IOUtils.closeQuietly(distributedFileSystem);
            }
        } else {
            super.cacheFile(filePath);
        }
    }
}
