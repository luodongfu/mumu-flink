package com.lovecws.mumu.flink.streaming.sink;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.util.DateUtils;
import com.lovecws.mumu.flink.common.util.MapFieldUtil;
import com.lovecws.mumu.flink.common.util.SftpParameter;
import com.lovecws.mumu.flink.common.util.SftpUtils;
import com.jcraft.jsch.JSchException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.*;

/**
 * @program: act-able
 * @description: 将解析结果存储到sftp服务器上
 * @author: 甘亮
 * @create: 2019-06-11 13:21
 **/
@Slf4j
@ModularAnnotation(type = "sink", name = "sftp")
public class SftpSink extends ScheduleFileSink {

    private SftpParameter sftpParameter;
    private String uploadDir;
    // 文件名开头的标记，例如440000_atd，不加可能产生文件上传到部的错位bug
    private String fileNameBeginMark;

    public SftpSink(Map<String, Object> configMap) {
        super(configMap);

        Map defaultsConfigMap = (Map) configMap.get("defaults");
        fileNameBeginMark = MapFieldUtil.getMapField(defaultsConfigMap, "provice.code").toString()
                + "_" + MapUtils.getString(configMap, "event", "");

        //初始化sftp路径
        uploadDir = MapUtils.getString(configMap, "uploadDir", "").toString().replace("\\", "/");

        sftpParameter = new SftpParameter();
        sftpParameter.hostName = MapUtils.getString(configMap, "ip", MapFieldUtil.getMapField(configMap, "defaults.sftp.ip").toString()).toString();
        sftpParameter.userName = MapUtils.getString(configMap, "user", MapFieldUtil.getMapField(configMap, "defaults.sftp.user").toString()).toString();
        sftpParameter.passWord = MapUtils.getString(configMap, "password", MapFieldUtil.getMapField(configMap, "defaults.sftp.password").toString()).toString();
        sftpParameter.port = MapUtils.getIntValue(configMap, "port", Integer.parseInt(MapFieldUtil.getMapField(configMap, "defaults.sftp.port").toString()));
        sftpParameter.sftpKeyFile = MapUtils.getString(configMap, "keyFile", "").toString();
        sftpParameter.conFTPTryCount = 3;
    }

    @Override
    public void executeScheduleTask(List<File> files) {
        try {
            //多文件上传
            List<String> storages = new ArrayList<>();
            for (String baseDir : uploadDir.split(",")) {
                if (!baseDir.endsWith("/")) baseDir = baseDir + "/";
                String storageDir = baseDir + DateUtils.getCurrentDayStr() + "/";
                SftpUtils.makeDir(sftpParameter, storageDir);
                storages.add(storageDir);
            }
            //sftp上传
            for (File file : files) {
                for (String storageDir : storages) {
                    //将源文件保存到sftp服务器上
                    sftpParameter.uploadPath = storageDir;
                    String uuidkey = UUID.randomUUID().toString().replace("-", "").substring(0, 6);
                    String uploadFileName = storageDir + fileNameBeginMark + "_" + DateUtils.formatDate(new Date(), "yyyyMMddHHmmssSSS") + uuidkey + "." + fileType;
                    Boolean flag = SftpUtils.uploadFile(sftpParameter, file, uploadFileName);
                    if (flag) {
                        log.info("storage sftp success: " + uploadFileName);
                    } else {
                        throw new JSchException();
                    }
                }
                FileUtils.deleteQuietly(file);
            }
        } catch (Exception ex) {
            try {
                sftpParameter.release();
            } catch (JSchException e) {
                log.error(e.getLocalizedMessage(), e);
            }
        }
    }
}
