package com.lovecws.mumu.flink.common.util;

import com.jcraft.jsch.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;


public class SftpUtils implements Serializable {

    private static org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(SftpUtils.class);

    private static final long M1 = 1024 * 1024;

    /**
     * connect to SFTP Server
     * If can't connect to SFTP , it will re-try
     *
     * @param para
     * @param count connect retry count
     * @return isConnected
     * @throws InterruptedException
     * @throws JSchException
     */
    public static Boolean connectSFTP(SftpParameter para, Integer count) throws InterruptedException, JSchException {
        Boolean ret = false;
        if (para == null)
            return ret;

        count = para.checkCount();

        if (para.isConnected)
            return true;
        if (StringUtils.isEmpty(para.hostName) || para.port == 0
                || para.userName == null || para.passWord == null) {
            log.error("SFTP connect parameter error !");
            para.release();
            return ret;
        }

        ChannelSftp csftp = null;
        Session session = null;
        Channel channel = null;
        JSch jsch = new JSch();
        //if key file is not null
        if (StringUtils.isNotBlank(para.sftpKeyFile)) {
            if (StringUtils.isBlank(para.passWord)) {
                jsch.addIdentity(para.sftpKeyFile);
            } else {
                jsch.addIdentity(para.sftpKeyFile, para.passWord);
            }
        }
        //skip key checking
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        session = jsch.getSession(para.userName, para.hostName, para.port);
        session.setConfig(config);
        if (StringUtils.isBlank(para.sftpKeyFile)) {
            session.setPassword(para.passWord);
        }

        int i = 0;
        while (true) {
            i++;
            try {
                session.connect();
                channel = session.openChannel("sftp");
                channel.connect();
                csftp = (ChannelSftp) channel;
                ret = true;
                para.isConnected = true;
                para.sftp = csftp;
                log.info("Successfully connect to " + para.hostName + ":" + para.port + " with " + para.userName);
                break;
            } catch (Exception ex) {
                log.error(ex);
                para.isConnected = false;
                if (i >= count) {
                    log.info(para.toString());
                    log.error(
                            "Can't connect SFTP Server, have already tried" + i
                                    + "times!");
                    try {
                        int interval = para.checkInterval();
                        Thread.sleep(interval);
                    } catch (Exception e) {
                        log.error(ex);
                    }
                    break;
                } else {
                    try {
                        int interval = para.checkInterval();
                        log.error(
                                "Can't connect SFTP Server，waiting for "
                                        + interval + "seconds try again!");
                        Thread.sleep(interval);
                    } catch (InterruptedException e) {
                        log.error(e);
                        break;
                    }
                }

            }

        }// end of while
        return ret;
    }

    /**
     * get SFTP Server file list .
     *
     * @param para
     * @return List<ChannelSftp.LsEntry>
     * @throws SftpException
     * @throws JSchException
     * @throws InterruptedException
     */
    @SuppressWarnings("unchecked")
    public static List<ChannelSftp.LsEntry> getFiles(SftpParameter para) throws InterruptedException, JSchException, SftpException {
        List<ChannelSftp.LsEntry> files = new ArrayList<ChannelSftp.LsEntry>();
        if (para != null) {
            if (connectSFTP(para, para.conFTPTryCount)) {
                ChannelSftp cs = para.sftp;
                Vector<ChannelSftp.LsEntry> fs = null;
                fs = cs.ls(para.downloadPath);
                for (ChannelSftp.LsEntry entry : fs) {
                    if (!entry.getAttrs().isDir()) {
                        files.add(entry);
                    }
                }
            }

        }
        return files;
    }

    /**
     * download files from SFTP
     *
     * @param para
     * @return
     * @throws InterruptedException
     * @throws JSchException
     * @throws SftpException
     * @throws IOException
     */
    public static Boolean downloadFiles(SftpParameter para) throws InterruptedException, JSchException, SftpException {
        Boolean ret = true;
        List<ChannelSftp.LsEntry> files = getFiles(para);
        if (!para.isConnected) connectSFTP(para, para.conFTPTryCount);
        for (ChannelSftp.LsEntry file : files) {
            ChannelSftp cs = para.sftp;
            if (!para.downloadPath.endsWith("/")) para.downloadPath = para.downloadPath + "/";
            if (!para.savePath.endsWith("/")) para.savePath = para.savePath + "/";
            String fromFile = para.downloadPath + file.getFilename();
            String toFile = para.savePath + file.getFilename();
            cs.get(fromFile, toFile);
        }
        return ret;
    }

    /**
     * download file from SFTP
     *
     * @param para
     * @return
     * @throws InterruptedException
     * @throws JSchException
     * @throws SftpException
     * @throws IOException
     */
    public static Boolean downloadFile(SftpParameter para) throws InterruptedException, JSchException, SftpException, IOException {
        Boolean ret = true;
        if (!para.isConnected) connectSFTP(para, para.conFTPTryCount);
        ChannelSftp cs = para.sftp;
        final String fromFile = para.downloadPath;
        String toFile = para.savePath;
        cs.get(fromFile, toFile);
        return ret;
    }

    /**
     * download file from SFTP
     *
     * @param para
     * @return
     * @throws InterruptedException
     * @throws JSchException
     * @throws SftpException
     * @throws IOException
     */
    public static Boolean downloadFileWithMonitor(SftpParameter para) throws InterruptedException, JSchException, SftpException, IOException {
        Boolean ret = true;
        if (!para.isConnected) connectSFTP(para, para.conFTPTryCount);
        ChannelSftp cs = para.sftp;
        final String fromFile = para.downloadPath;
        String toFile = para.savePath;
        cs.get(fromFile, toFile, new SftpProgressMonitor() {
            private long transfered = 0L;
            private long currentSize = 0L;

            @Override
            public void init(int op, String src, String dest, long max) {
                System.out.println("[" + fromFile + "] Transferring begin.");
            }

            @Override
            public boolean count(long count) {
                transfered = transfered + count;
                currentSize += count;

                //当数据加载到10M的时候 打印当前的数据量
                if (currentSize / M1 >= 10) {
                    currentSize = 0L;
                    System.out.println("[" + fromFile + "] Currently transferred total size: " + Math.round(transfered / M1) + " mb");
                }
                return true;
            }

            @Override
            public void end() {
                System.out.println("[" + fromFile + "] Transferring done.");
            }
        });
        return ret;
    }

    public static List<ChannelSftp.LsEntry> getDirFiles(SftpParameter para) throws InterruptedException, JSchException, SftpException {
        List<ChannelSftp.LsEntry> files = new ArrayList<ChannelSftp.LsEntry>();
        if (para != null) {
            if (connectSFTP(para, para.conFTPTryCount)) {
                ChannelSftp cs = para.sftp;
                Vector<ChannelSftp.LsEntry> fs = null;
                fs = cs.ls(para.downloadPath);
                for (ChannelSftp.LsEntry entry : fs) {
                    if (entry.getFilename().equals(".") || entry.getFilename().equals("..")) {
                        continue;
                    }
                    files.add(entry);
                }
            }

        }
        return files;
    }

    /**
     * get sftp file content
     *
     * @param para
     * @return
     * @throws InterruptedException
     * @throws JSchException
     * @throws SftpException
     * @throws IOException
     */
    public static String getContent(SftpParameter para) throws InterruptedException, JSchException, SftpException, IOException {
        if (!para.isConnected) connectSFTP(para, para.conFTPTryCount);
        ChannelSftp cs = para.sftp;
        InputStream inputStream = null;
        try {
            if (StringUtils.isEmpty(para.savePath)) {
                inputStream = cs.get(para.downloadPath);
            } else {
                cs.get(para.downloadPath, para.savePath);
                inputStream = new FileInputStream(para.savePath);
            }
            byte[] bs = new byte[inputStream.available()];
            inputStream.read(bs);
            return new String(bs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
        return null;
    }

    /**
     * get sftp file content
     *
     * @param para
     * @return
     * @throws InterruptedException
     * @throws JSchException
     */
    public static byte[] getBytes(SftpParameter para) throws InterruptedException, JSchException {
        if (!para.isConnected) connectSFTP(para, para.conFTPTryCount);
        ChannelSftp cs = para.sftp;
        InputStream inputStream = null;
        try {
            inputStream = cs.get(para.downloadPath);
            byte[] bs = new byte[inputStream.available()];
            inputStream.read(bs);
            return bs;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
        return null;
    }

    /**
     * upload file to SFTP
     *
     * @param para           sftpParameter
     * @param file           文件
     * @param uploadFileName 上传文件名称
     * @return
     * @throws InterruptedException
     * @throws JSchException
     * @throws SftpException
     */
    public static Boolean uploadFile(SftpParameter para, File file, String uploadFileName) throws InterruptedException, JSchException, SftpException {
        Boolean ret = false;
        if (!file.exists()) return ret;
        if (connectSFTP(para, para.conFTPTryCount)) {
            ChannelSftp cs = para.sftp;
            if (!para.uploadPath.endsWith("/")) para.uploadPath = para.uploadPath + "/";
            //if (para.uploadPath.startsWith("/")) para.uploadPath = para.uploadPath.substring(1);
            // 临时文件名
            final String tmpName = uploadFileName + ".tmp";
            cs.put(file.getAbsolutePath(), tmpName);
            cs.rename(tmpName, uploadFileName);
            ret = true;
        }
        return ret;
    }

    /**
     * upload files to SFTP server .
     *
     * @param para
     * @return
     * @throws SftpException
     * @throws JSchException
     * @throws InterruptedException
     */
    public static Boolean uploadFiles(SftpParameter para, List<File> files) throws InterruptedException, JSchException, SftpException {
        Boolean ret = true;
        if (files == null || files.size() <= 0) return ret;
        int i = 0;
        for (File file : files) {
            if (!uploadFile(para, file, para.uploadPath + file.getName())) i++;
        }
        if (i > 0) {
            log.error(i + " items were uploaded failed!");
            ret = false;
        }
        return ret;
    }


    /**
     * delete FTP server file .
     *
     * @param para
     * @param filepath file path must be /home/user/text.xxx
     * @return true or false
     * @throws SftpException
     * @throws JSchException
     * @throws InterruptedException
     */
    public static Boolean deleteFile(SftpParameter para, String filepath) throws InterruptedException, JSchException, SftpException {
        Boolean ret = false;
        if (connectSFTP(para, para.conFTPTryCount)) {
            para.sftp.rm(filepath);
            ret = true;
        }
        return ret;
    }


    /**
     * delete SFTP  files .
     *
     * @param para
     * @return true or false
     * @throws SftpException
     * @throws JSchException
     * @throws InterruptedException
     */
    public static Boolean deleteFiles(SftpParameter para, List<String> list) throws InterruptedException, JSchException, SftpException {
        Boolean ret = true;
        if (list == null || list.size() <= 0) ret = false;
        int i = 0;
        for (String s : list) {

            if (!deleteFile(para, s)) i++;
        }
        if (i > 0) {
            log.equals(i + " items were deleted failed!");
            ret = false;
        }
        return ret;
    }

    /**
     * is file exist in SFTP
     *
     * @param para
     * @param path
     * @param file
     * @return
     * @throws InterruptedException
     * @throws JSchException
     * @throws SftpException
     */
    @SuppressWarnings("unchecked")
    public static boolean isFileExist(SftpParameter para, String path, String file) throws InterruptedException, JSchException, SftpException {
        if (connectSFTP(para, para.conFTPTryCount)) {
            Vector<ChannelSftp.LsEntry> files = null;
            ChannelSftp cs = para.sftp;
            files = cs.ls(path);
            for (ChannelSftp.LsEntry entry : files) {
                if (!entry.getAttrs().isDir()
                        && entry.getFilename().equals(file)) {
                    return true;
                }
            }
        }
        return false;
    }


    /**
     * is file exist in SFTP
     *
     * @param para
     * @param path
     * @return
     * @throws InterruptedException
     * @throws JSchException
     * @throws SftpException
     */
    @SuppressWarnings("unchecked")
    public static boolean isDirExist(SftpParameter para, String path, String dirname) throws InterruptedException, JSchException, SftpException {
        if (connectSFTP(para, para.conFTPTryCount)) {
            Vector<ChannelSftp.LsEntry> files = null;
            ChannelSftp cs = para.sftp;
            files = cs.ls(path);
            for (ChannelSftp.LsEntry entry : files) {
                if (entry.getAttrs().isDir()
                        && entry.getFilename().equals(dirname)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * make dir
     *
     * @param para
     * @param makeDir like /home/ccc/bbb
     * @throws InterruptedException
     * @throws JSchException
     * @throws SftpException
     */
    public static void makeDir(SftpParameter para, String makeDir) throws InterruptedException, JSchException, SftpException {
        if ("".equals(makeDir) || makeDir == null) return;
        connectSFTP(para, para.conFTPTryCount);
        ChannelSftp cs = para.sftp;
        String[] makedirs = makeDir.split("\\/");
        String ls = "./";
        for (String dir : makedirs) {
            if ("".equals(dir) || dir == null) {
                continue;
            }
            //目录不存在 则创建
            if (!isDirExist(para, ls, dir)) {
                cs.mkdir(dir);
            }
            cs.cd(dir);
        }
        log.info("create dir :" + makeDir);
        para.release();
    }
}