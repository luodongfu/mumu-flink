package com.lovecws.mumu.flink.streaming.source;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.util.FtpUtils;
import com.lovecws.mumu.flink.common.util.MapFieldUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.net.ftp.FTPFile;

import java.util.List;
import java.util.Map;

/**
 * @program: act-able
 * @description: 从ftp拉取数据。
 * 数据取完之后怎么处理防止下次重复取。1、如果ftp有操作数据的权限可以修改文件名称。2、redis缓存已经处理过的文件，
 * 同时不处理过期数据
 * @author: 甘亮
 * @create: 2019-06-17 17:51
 **/
@Slf4j
@ModularAnnotation(type = "source", name = "ftp")
public class FtpFileSource extends BaseFileSource {

    private FtpUtils ftpUtils;

    public FtpFileSource(Map<String, Object> configMap) {
        super(configMap);
        String host = MapUtils.getString(configMap, "host", MapFieldUtil.getMapField(configMap, "defaults.ftp.host").toString());
        int port = Integer.parseInt(configMap.getOrDefault("port", MapFieldUtil.getMapField(configMap, "defaults.ftp.port")).toString());
        String user = MapUtils.getString(configMap, "user", MapFieldUtil.getMapField(configMap, "defaults.ftp.user").toString());
        String password = MapUtils.getString(configMap, "password", MapFieldUtil.getMapField(configMap, "defaults.ftp.password").toString()).toString();

        ftpUtils = new FtpUtils(host, port, user, password);
    }


    /**
     * 递归收集ftp文件
     *
     * @param path         ftp收集的根目录
     * @param filePaths    收集的ftp文件路径
     * @param collectCount 一次收集的数量
     */
    @Override
    public void collectFiles(String path, List<String> filePaths, int collectCount) {
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        FTPFile[] ftpFiles = ftpUtils.listFiles(path);
        for (FTPFile ftpFile : ftpFiles) {
            String currentFile = path + "/" + ftpFile.getName();
            if (ftpFile.isDirectory()) {
                collectFiles(currentFile, filePaths, collectCount);
            } else if (ftpFile.isFile()) {
                //ftp文件最后修改时间
                long timeInMillis = ftpFile.getTimestamp().getTimeInMillis();
                //过滤正在写入的文件(当前时间-文件最后的修改时间>30秒)
                if (System.currentTimeMillis() - timeInMillis < 30000) {
                    log.debug("文件过滤,文件正在写入[" + currentFile + "],或者最后修改时间小于30秒");
                    continue;
                }
                if (filterFile(currentFile)) continue;

                log.info("collect ftp file :" + currentFile);

                filePaths.add(currentFile);
                if (collectCount > 0 && filePaths.size() >= collectCount) {
                    break;
                }
            }
        }
    }

    @Override
    public void handleFile(SourceContext<Object> ctx, String filePath, byte[] bytes, String fileType) {
        try {
            byte[] ftpDownFileBytes = ftpUtils.ftpDownFile(filePath);
            super.handleFile(ctx, filePath, ftpDownFileBytes, fileType);
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage());
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
            ftpUtils.delFtpFile(filePath);
        } else {
            super.cacheFile(filePath);
        }
    }
}
