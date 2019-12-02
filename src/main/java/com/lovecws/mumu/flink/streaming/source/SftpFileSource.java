package com.lovecws.mumu.flink.streaming.source;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.util.MapFieldUtil;
import com.lovecws.mumu.flink.common.util.SftpParameter;
import com.lovecws.mumu.flink.common.util.SftpUtils;
import com.jcraft.jsch.ChannelSftp;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @program: act-able
 * @description: 从sftp拉取数据。
 * 数据取完之后怎么处理防止下次重复取。1、如果ftp有操作数据的权限可以修改文件名称。2、redis缓存已经处理过的文件，
 * 同时不处理过期数据
 * @author: 甘亮
 * @create: 2019-06-17 17:51
 **/
@Slf4j
@ModularAnnotation(type = "source", name = "sftp")
public class SftpFileSource extends BaseFileSource {

    private SftpParameter sftpParameter = new SftpParameter();


    private List<String> sftpFiles = new ArrayList<>();
    private Map<String, List<String>> sftpFileMap = new HashMap<>();

    public SftpFileSource(Map<String, Object> configMap) {
        super(configMap);

        sftpParameter.hostName = configMap.getOrDefault("host", MapFieldUtil.getMapField(configMap, "defaults.sftp.host", "localhost")).toString();
        sftpParameter.port = Integer.parseInt(configMap.getOrDefault("port", MapFieldUtil.getMapField(configMap, "defaults.sftp.port", "21")).toString());
        sftpParameter.userName = configMap.getOrDefault("user", MapFieldUtil.getMapField(configMap, "defaults.sftp.user")).toString();
        sftpParameter.passWord = configMap.getOrDefault("password", MapFieldUtil.getMapField(configMap, "defaults.sftp.password")).toString();
        sftpParameter.sftpKeyFile = configMap.getOrDefault("keyFile", MapFieldUtil.getMapField(configMap, "defaults.sftp.keyFile")).toString();
        sftpParameter.conFTPTryCount = 3;
    }

    /**
     * 递归收集sftp文件
     *
     * @param path         ftp收集的根目录
     * @param filePaths    收集的ftp文件路径
     * @param collectCount 一次收集的数量
     */
    @Override
    public void collectFiles(String path, List<String> filePaths, int collectCount) {
        SftpParameter para = sftpParameter;
        if (path.endsWith("/")) path = path.substring(0, path.length() - 1);
        try {
            para.downloadPath = path;
            List<ChannelSftp.LsEntry> dirFiles = new ArrayList<>();
            try {
                dirFiles = SftpUtils.getDirFiles(para);
            } catch (Exception ex) {
                log.error("sftp dir not exists " + path + " " + ex.getMessage());
            }
            for (ChannelSftp.LsEntry lsEntry : dirFiles) {
                String currentFile = path + "/" + lsEntry.getFilename();
                if (lsEntry.getAttrs().isDir()) {
                    collectFiles(currentFile, filePaths, collectCount);
                } else {
                    if (collectCount > 0 && filePaths.size() >= collectCount) {
                        break;
                    }
                    int mTime = lsEntry.getAttrs().getMTime();
                    //过滤正在写入的文件(当前时间-文件最后的修改时间>30秒)
                    if (System.currentTimeMillis() - mTime < 30000) {
                        log.debug("文件过滤,文件正在写入[" + currentFile + "],或者最后修改时间小于30秒");
                        continue;
                    }
                    if (filterFile(currentFile)) continue;

                    log.info("collect sftp file :" + currentFile);

                    filePaths.add(currentFile);
                }
            }
        } catch (Exception ex) {
            log.error("collectSftpFilesError", ex);
        }
    }

    @Override
    public void handleFile(SourceContext<Object> ctx, String filePath, byte[] bytes, String fileType) {
        try {
            sftpParameter.downloadPath = filePath;
            byte[] sftpFileBytes = SftpUtils.getBytes(sftpParameter);
            super.handleFile(ctx, filePath, sftpFileBytes, fileType);
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
            try {
                SftpUtils.deleteFile(sftpParameter, filePath);
            } catch (Exception ex) {
                log.error(ex.getLocalizedMessage());
            }
        } else {
            super.cacheFile(filePath);
        }
    }
}
