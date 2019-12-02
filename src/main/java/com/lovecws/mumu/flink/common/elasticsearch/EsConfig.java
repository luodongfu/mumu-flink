package com.lovecws.mumu.flink.common.elasticsearch;

import java.io.Serializable;

/**
 * @program: jmr
 * @description: es配置文件信息
 * @author: 甘亮
 * @create: 2018-10-29 15:31
 **/
public class EsConfig implements Serializable {

    //es集群信息
    private String host;
    private String port;
    private String clusterName;

    //数据插入信息
    private String insertType = "sync";//插入es的方式，支持同步(sync)、异步(async)
    private int insertSleep;//#插入数据完成之后休眠固定的时间，防止es插入数据太频繁导致es崩溃,单位为秒
    private int numberShards = 5;//创建es的分片数量，默认3
    private int numberReplicas = 0;//创建es的副本数量，默认0
    private int totalShardsPerNode = 2;//创建es指定每一个节点最多分配多少个分片
    private boolean disableAllocation = false;//是否禁用es自动平均分片，开启容易导致数据一直在allocation，导致节点io异常

    //客户端连接信息
    private boolean tcpCompress = true;
    private boolean sniff = false;
    private String pingTimeout = "120s";
    private String refreshInterval = "120s";

    //连接池信息
    private int poolMaxIdle = 10;
    private int poolMaxNum = 10;
    private int poolMinIdle = 5;
    private boolean poolTestReturn = false;
    private boolean poolTestBorrow = false;
    private boolean poolTestCreate = true;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getInsertType() {
        return insertType;
    }

    public void setInsertType(String insertType) {
        this.insertType = insertType;
    }

    public int getInsertSleep() {
        return insertSleep;
    }

    public void setInsertSleep(int insertSleep) {
        this.insertSleep = insertSleep;
    }

    public int getNumberShards() {
        return numberShards;
    }

    public void setNumberShards(int numberShards) {
        this.numberShards = numberShards;
    }

    public int getNumberReplicas() {
        return numberReplicas;
    }

    public void setNumberReplicas(int numberReplicas) {
        this.numberReplicas = numberReplicas;
    }

    public int getTotalShardsPerNode() {
        return totalShardsPerNode;
    }

    public void setTotalShardsPerNode(int totalShardsPerNode) {
        this.totalShardsPerNode = totalShardsPerNode;
    }

    public boolean isDisableAllocation() {
        return disableAllocation;
    }

    public void setDisableAllocation(boolean disableAllocation) {
        this.disableAllocation = disableAllocation;
    }

    public boolean isTcpCompress() {
        return tcpCompress;
    }

    public void setTcpCompress(boolean tcpCompress) {
        this.tcpCompress = tcpCompress;
    }

    public boolean isSniff() {
        return sniff;
    }

    public void setSniff(boolean sniff) {
        this.sniff = sniff;
    }

    public String getPingTimeout() {
        return pingTimeout;
    }

    public void setPingTimeout(String pingTimeout) {
        this.pingTimeout = pingTimeout;
    }

    public String getRefreshInterval() {
        return refreshInterval;
    }

    public void setRefreshInterval(String refreshInterval) {
        this.refreshInterval = refreshInterval;
    }

    public int getPoolMaxIdle() {
        return poolMaxIdle;
    }

    public void setPoolMaxIdle(int poolMaxIdle) {
        this.poolMaxIdle = poolMaxIdle;
    }

    public int getPoolMaxNum() {
        return poolMaxNum;
    }

    public void setPoolMaxNum(int poolMaxNum) {
        this.poolMaxNum = poolMaxNum;
    }

    public int getPoolMinIdle() {
        return poolMinIdle;
    }

    public void setPoolMinIdle(int poolMinIdle) {
        this.poolMinIdle = poolMinIdle;
    }

    public boolean isPoolTestReturn() {
        return poolTestReturn;
    }

    public void setPoolTestReturn(boolean poolTestReturn) {
        this.poolTestReturn = poolTestReturn;
    }

    public boolean isPoolTestBorrow() {
        return poolTestBorrow;
    }

    public void setPoolTestBorrow(boolean poolTestBorrow) {
        this.poolTestBorrow = poolTestBorrow;
    }

    public boolean isPoolTestCreate() {
        return poolTestCreate;
    }

    public void setPoolTestCreate(boolean poolTestCreate) {
        this.poolTestCreate = poolTestCreate;
    }
}
