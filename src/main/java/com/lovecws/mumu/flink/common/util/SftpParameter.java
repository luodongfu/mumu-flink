package com.lovecws.mumu.flink.common.util;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import java.io.Serializable;


public class SftpParameter implements Serializable {

    public String hostName;

    public int port = 0;

    public String userName;

    public String passWord;

    public String sftpKeyFile;

    public String downloadPath;

    public String savePath;

    public String uploadPath;

    public static int CON_FTP_TRYCOUNT_DEFAULT = 3;

    public static int CON_FTP_TRYINTERVAL_DEFAULT = 1;

    public int conFTPTryCount = 0;

    public int conFTPTryInterval = 1;

    public ChannelSftp sftp = null;

    public Boolean isConnected = false;

    public int checkInterval() {
        if (0 == this.conFTPTryInterval)
            return CON_FTP_TRYINTERVAL_DEFAULT * 1000;
        else
            return this.conFTPTryInterval * 1000;
    }

    public int checkCount() {
        if (this.conFTPTryCount <= 0)
            return CON_FTP_TRYCOUNT_DEFAULT;
        else
            return this.conFTPTryCount;
    }

    public void release() throws JSchException {
        if (sftp != null) {
            Session session = sftp.getSession();
            try {
                if (sftp.isConnected()) {
                    sftp.disconnect();
                    sftp.quit();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                if (session != null && session.isConnected()) {
                    session.disconnect();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        isConnected = false;
    }

    @Override
    public String toString() {
        return "SftpParameter{" +
                "hostName='" + hostName + '\'' +
                ", port=" + port +
                ", userName='" + userName + '\'' +
                ", passWord='" + passWord + '\'' +
                ", sftpKeyFile='" + sftpKeyFile + '\'' +
                ", downloadPath='" + downloadPath + '\'' +
                ", savePath='" + savePath + '\'' +
                ", uploadPath='" + uploadPath + '\'' +
                ", conFTPTryCount=" + conFTPTryCount +
                ", conFTPTryInterval=" + conFTPTryInterval +
                ", sftp=" + sftp +
                ", isConnected=" + isConnected +
                '}';
    }
}