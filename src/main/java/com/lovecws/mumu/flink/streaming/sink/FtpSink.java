package com.lovecws.mumu.flink.streaming.sink;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.util.DateUtils;
import com.lovecws.mumu.flink.common.util.FtpUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @program: act-able
 * @description: 将解析结果发送到ftp服务器上
 * @author: 甘亮
 * @create: 2019-06-11 13:11
 **/
@Slf4j
@ModularAnnotation(type = "sink", name = "ftp")
public class FtpSink extends ScheduleFileSink {

    private FtpUtils ftpUtils;
    private String uploadDir;

    public FtpSink(Map<String, Object> configMap) {
        super(configMap);
        //初始化ftp路径
        String ip = MapUtils.getString(configMap, "ip", "localhost");
        int port = MapUtils.getIntValue(configMap, "port", 21);
        String user = MapUtils.getString(configMap, "user", "root").toString();
        String password = MapUtils.getString(configMap, "password", "123456").toString();

        ftpUtils = new FtpUtils(ip, port, user, password);
        uploadDir = MapUtils.getString(configMap, "uploadDir", "").toString().replace("\\", "/");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void executeScheduleTask(List<File> files) {
        try {
            //创建目录
            String workingDirectory = ftpUtils.ftpClient.printWorkingDirectory();
            List<String> storageDirList = new ArrayList<>();
            for (String ftpUploaddir : uploadDir.split(",")) {
                if (!ftpUploaddir.endsWith("/")) ftpUploaddir = ftpUploaddir + "/";
                String storageDir = ftpUploaddir + DateUtils.getCurrentDayStr() + "/";
                try {
                    ftpUtils.mkdirs(storageDir);
                    ftpUtils.ftpClient.changeWorkingDirectory(workingDirectory);
                } catch (IOException e) {
                    log.error(e.getLocalizedMessage());
                }
                storageDirList.add(storageDir);
            }
            //多文件上传
            for (File file : files) {
                for (String storageDir : storageDirList) {
                    ftpUtils.ftpClient.enterLocalPassiveMode();
                    ftpUtils.ftpClient.changeWorkingDirectory(workingDirectory);
                    ftpUtils.ftpClient.changeWorkingDirectory(storageDir);
                    boolean success = ftpUtils.upload(ftpUtils.ftpClient, file);
                    if (success) {
                        log.info("storage ftp file success: " + storageDir + file.getName());
                    }
                }
                FileUtils.deleteQuietly(file);
            }
        } catch (IOException e) {
            log.error(e.getLocalizedMessage());
        } finally {
            ftpUtils.releaseClient();
        }
    }
}
