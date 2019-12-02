/**
 * Project Name:smcs_zj
 * File Name:FtpUtil.java
 * Date:2014年1月22日上午11:14:13
 */

package com.lovecws.mumu.flink.common.util;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.SocketException;

/**
 * ClassName:FtpUtil <br/>
 * Date:     2014年1月22日 上午11:14:13 <br/>
 *
 * @author quanli
 * @see
 * @since JDK 1.6
 */
public class FtpUtils implements Serializable {

    private final String FTP_CHART_SET = "iso-8859-1";
    public FTPClient ftpClient = null;
    private Logger logger = Logger.getLogger(FtpUtils.class);
    private boolean loginFlag = false;

    public FtpUtils(String ip, int port, String user, String password) {
        this.ftpClient = new FTPClient();
        try {
            boolean login = login(ip, port, user, password);
            if (login) {
                logger.info("Ftp连接成功!");
            }
        } catch (SocketException e) {
            logger.error("FTP创建socket失败!", e);
        } catch (IOException e) {
            logger.error("FTP连接IO异常!", e);
        }
    }

    /**
     * containSuffix:判断文件是否有过滤后缀. <br/>
     *
     * @param name   文件名
     * @param suffix 过滤后缀
     * @return
     * @author quanli
     * @since JDK 1.6
     */
    private static boolean containSuffix(String name, String suffix) {
        if (suffix == null) {
            return false;
        }
        String[] suffixs = suffix.split(";");
        for (String temp : suffixs) {
            if (name.endsWith(temp)) {
                return true;
            }
        }
        return false;
    }

    /**
     * ftpDownFile:从Ftp服务器上下载文件
     *
     * @param filePath
     * @return
     * @author dengqw
     * @since JDK 1.6
     */
    public byte[] ftpDownFile(String filePath) {
        byte[] data = null;
        InputStream inputStream = null;
        ByteArrayOutputStream baos = null;
        try {
            if (loginFlag) {
                baos = new ByteArrayOutputStream();
                inputStream = ftpClient.retrieveFileStream(filePath);
                if (inputStream != null) {
                    int bt;
                    while ((bt = inputStream.read()) != -1) {
                        baos.write(bt);
                    }
                }
                data = baos.toByteArray();
            } else {
                throw new Exception("ftp 未成功连接");
            }

        } catch (Exception e) {
            logger.error("FTP文件下载异常!", e);
            return null;
        } finally {
            try {
                if (baos != null) {
                    baos.close();
                }
                if (inputStream != null) {
                    inputStream.close();
                }
                ftpClient.completePendingCommand();
            } catch (IOException e) {
                logger.error("FTP IO异常!", e);
            }
        }
        return data;
    }

    /**
     * ftpUploadFile:上传文件到FTP
     *
     * @param file 文件
     * @param path 切换的目录
     * @return
     * @author dengqw
     * @since JDK 1.6
     */
    public boolean ftpUploadFile(File file, String path) {
        try {
            if (loginFlag) {
                boolean flag = changeDirectory(path);
                if (!flag) {
                    return false;
                }
                upload(ftpClient, file);
            }
        } catch (Exception e) {
            logger.error("error", e);
            return false;
        }
        return true;
    }

    /**
     * ftpUploadFile:上传文件到FTP
     *
     * @param workDir   切换的目录
     * @param file      文件
     * @param fileBytes 文件字节
     * @return
     */
    public boolean uploadBytes(String workDir, String file, byte[] fileBytes) {
        if (loginFlag) {
            try {
                ftpClient.enterLocalPassiveMode();
                boolean workingDirectory = ftpClient.changeWorkingDirectory(workDir);
                logger.info("changeWorkingDirectory " + workDir + " " + workingDirectory);
                OutputStream outputStream = ftpClient.storeFileStream(file);
                if (outputStream != null) {
                    outputStream.write(fileBytes);
                    outputStream.flush();
                } else {
                    return false;
                }
                /*if (!ftpClient.completePendingCommand()) {
                    logger.info("file upload fail " + workDir + file);
                }*/
            } catch (Exception e) {
                logger.error(e.getLocalizedMessage(), e);
                return false;
            }
        }
        return true;
    }

    /**
     * @param file 上传的文件或文件夹
     * @throws Exception
     */
    public boolean upload(FTPClient ftp, File file) throws IOException {
        boolean flag = false;
        if (file.isDirectory()) {
            ftp.makeDirectory(file.getName());
            ftp.changeWorkingDirectory(file.getName());
            String[] files = file.list();
            if (files == null) {
                return false;
            }
            for (int i = 0; i < files.length; i++) {
                File file1 = new File(file.getPath() + File.separator + files[i]);
                if (file1.isDirectory()) {
                    upload(ftp, file1);
                    ftp.changeToParentDirectory();
                } else {
                    File file2 = new File(file.getPath() + File.separator + files[i]);
                    FileInputStream input = new FileInputStream(file2);
                    flag = ftp.storeFile(file2.getName(), input);
                    input.close();
                }
            }
        } else {
            FileInputStream input = new FileInputStream(file);
            String ftpFileName = new String(file.getName().getBytes(), FTP_CHART_SET);
            flag = ftp.storeFile(ftpFileName, input);
            input.close();
        }
        logger.info("storefile " + file.getName() + " :" + (flag ? "success" : "fail"));
        return flag;
    }

    /**
     * login:登录ftp服务器. <br/>
     *
     * @param ip       服务器IP
     * @param port     服务器端口
     * @param user     用户
     * @param password 密码
     * @author quanli
     * @since JDK 1.6
     */
    public boolean login(String ip, int port, String user, String password) throws SocketException, IOException {
        int reply = 0;
        if (ftpClient != null) {
            reply = ftpClient.getReplyCode();
            if (FTPReply.isPositiveCompletion(reply)) {
                this.loginFlag = true;
                return loginFlag;
            }
            ftpClient.connect(ip, port);
//            ftpClient.setControlEncoding("UTF-8");
            ftpClient.login(user, password);
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
            ftpClient.enterLocalPassiveMode();
            ftpClient.setConnectTimeout(1 * 60 * 1000);//设置ftp连接超时时间
            ftpClient.setDataTimeout(5 * 60 * 1000);//文件上传超时时间
            ftpClient.setSoTimeout(60 * 1000); //设置命令响应时间
            reply = ftpClient.getReplyCode();

            if (FTPReply.isPositiveCompletion(reply)) {
                this.loginFlag = true;
                return loginFlag;
            } else {
                this.loginFlag = false;
                return loginFlag;
            }
        } else {
            this.loginFlag = false;
            return loginFlag;
        }
    }

    /**
     * releaseClient:释放ftp连接资源. <br/>
     *
     * @author quanli
     * @since JDK 1.6
     */
    public void releaseClient() {
        if (ftpClient != null) {
            try {
                ftpClient.disconnect();
            } catch (IOException e) {
                logger.error("FTP释放资源IO异常!", e);

            }
        }
    }

    public FTPFile[] listFiles(String dir) {
        try {
            ftpClient.enterLocalPassiveMode();
            if (dir == null || "".equals(dir)) {
                return ftpClient.listFiles();
            }
            return ftpClient.listFiles(dir);
        } catch (IOException e) {
            logger.error("error", e);
        }
        return null;
    }

    public boolean changeDirectory(String filePath) {
        boolean flag = false;
        try {
            ftpClient.enterLocalPassiveMode();
            if (StringUtils.isNotBlank(filePath)) {
                if (filePath.startsWith("/")) {
                    ftpClient.changeWorkingDirectory("/");
                    filePath = filePath.substring(1);
                }
                String[] paths = filePath.split("/");
                if (paths.length > 0) {
                    for (String p : paths) {
                        ftpClient.makeDirectory(new String(p.getBytes("UTF-8"), FTP_CHART_SET));
                        flag = ftpClient.changeWorkingDirectory(p + "/");
                        //logger.info("current working directory : " + ftpClient.printWorkingDirectory());
                        if (!flag) {
                            break;
                        }
                    }
                } else {
                    flag = true;
                }
            }
        } catch (Exception ex) {
            logger.error(ex.getLocalizedMessage(), ex);
        }

        logger.info(filePath + ":" + flag);
        return flag;
    }

    /**
     * delFtpFile:删除文件. <br/>
     *
     * @param filePath 特定的文件夹
     * @author quanli
     * @since JDK 1.6
     */
    public void delFtpDir(String filePath) {
        try {
            if (loginFlag) {
                FTPFile[] files = ftpClient.listFiles(filePath);
                for (FTPFile file : files) {
                    if (file.getName().equals(".") || file.getName().equals("..")) {
                        continue;
                    }
                    if (file.isDirectory()) {
                        delFtpFileNoClosed(filePath + "/" + file.getName());
                    } else if (file.isFile()) {
                        System.out.println(filePath + "/" + file.getName());
                        ftpClient.deleteFile(filePath + "/" + file.getName());
                    }
                }
                ftpClient.rmd(filePath);
            }
        } catch (Exception e) {
            logger.error("文件删除失败!", e);
        }
    }

    /**
     * delFtpFile:删除文件. <br/>
     *
     * @param filePath 特定的文件
     * @author quanli
     * @since JDK 1.6
     */
    public void delFtpFile(String filePath) {
        try {
            if (loginFlag) {
                ftpClient.deleteFile(filePath);
            }
        } catch (Exception e) {
            logger.error("文件删除失败!", e);
        }
    }

    public void delFtpFileNoClosed(String filePath) {
        try {
            if (loginFlag) {
                FTPFile[] files = ftpClient.listFiles(filePath);
                for (FTPFile file : files) {
                    if (file.getName().equals(".") || file.getName().equals("..")) {
                        continue;
                    }
                    if (file.isDirectory()) {
                        delFtpFileNoClosed(filePath + "/" + file.getName());
                    } else if (file.isFile()) {
                        ftpClient.deleteFile(filePath + "/" + file.getName());
                    }
                }
                ftpClient.rmd(filePath);
            }
        } catch (Exception e) {
            logger.error("文件删除失败!", e);
        }
    }

    public void mkdirs(String backupDir) {
        changeDirectory(backupDir);
    }
}

