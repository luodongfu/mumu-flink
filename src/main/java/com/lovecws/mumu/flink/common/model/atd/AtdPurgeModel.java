package com.lovecws.mumu.flink.common.model.atd;

import com.lovecws.mumu.flink.common.annotation.TableField;
import com.lovecws.mumu.flink.common.annotation.TableIndex;
import com.lovecws.mumu.flink.common.annotation.TablePartition;
import com.lovecws.mumu.flink.common.util.DateUtils;
import lombok.Data;

import java.io.Serializable;
import java.util.Date;

/**
 * @program: act-able
 * @description: atd清洗表
 * @author: 甘亮
 * @create: 2019-05-29 09:43
 **/

/**
 * 1  malicious_software
 * 2  loophole_scan
 * 3  bot_net
 * 4  web_injection
 * 5  extortion_virus
 * 6  mining_events
 * 7  command_execution
 * 8  trojan_backdoor
 * 9  brute_force
 * 10 spam_fraud
 * 11 dos_attack
 * 12 auth_infiltration
 * 13 intelligence_outreach
 * 14 loophole_attack         漏洞攻击
 * 15  abnormal_traffic        异常流量
 * 16  apt_attack              apt攻击
 */
@Data
//list分区表
/*@TablePartition(partition = true, partitionType = "list", partitionFields = "event_type_id", databaseType = "pg",
        partitionStragety = {"1:malicious_software", "2:loophole_scan", "3:bot_net", "4:web_injection",
                "5:extortion_virus", "6:mining_events", "7:command_execution", "8:trojan_backdoor", "9:brute_force", "10:spam_fraud", "11:dos_attack",
                "12:auth_infiltration", "13:intelligence_outreach",
                "14:loophole_attack", "15:abnormal_traffic", "16:apt_attack"})*/

@TablePartition(partition = true, partitionType = "range", partitionFields = {"event_type_id", "ds"}, databaseType = "pg",
        partitionStragety = {"1:malicious_software", "2:loophole_scan", "3:bot_net", "4:web_injection",
                "5:extortion_virus", "6:mining_events", "7:command_execution", "8:trojan_backdoor", "9:brute_force", "10:spam_fraud", "11:dos_attack",
                "12:auth_infiltration", "13:intelligence_outreach",
                "14:loophole_attack", "15:abnormal_traffic", "16:apt_attack"}, interval = "week", count = 1)
@TableIndex(indexing = true, indexType = "pk", indexFields = "id")
public class AtdPurgeModel implements Serializable {

    @TableField(name = "id", comment = "消息唯一性")
    private String id;

    @TableField(name = "event_type_id", comment = "事件类型分区字段")
    private String eventTypeId;

    @TableField(name = "ds", comment = "ds年月日时间分区表")
    private String ds;

    @TableField(name = "src_ip", comment = "源ip")
    private String srcIp;

    @TableField(name = "src_port", comment = "源端口")
    private String srcPort;

    @TableField(name = "src_corp_name", comment = "源ip企业名称")
    private String srcCorpName;
    @TableField(name = "src_industry", comment = "源ip行业分类")
    private String srcIndustry;
    @TableField(name = "src_ip_country", comment = "源IP所在国家名")
    private String srcIpCountry;
    @TableField(name = "src_ip_province", comment = "源IP所在省份")
    private String srcIpProvince;
    @TableField(name = "src_ip_city", comment = "源IP所在城市")
    private String srcIpCity;

    @TableField(name = "dst_ip", comment = "目的ip")
    private String dstIp;
    @TableField(name = "dst_port", comment = "目的端口")
    private String dstPort;
    @TableField(name = "corp_name", comment = "目的ip企业名称")
    private String corpName;
    @TableField(name = "industry", comment = "目的ip行业分类")
    private String industry;
    @TableField(name = "dst_ip_country", comment = "目的IP所在国家名")
    private String dstIpCountry;
    @TableField(name = "dst_ip_province", comment = "目的IP所在省份")
    private String dstIpProvince;
    @TableField(name = "dst_ip_city", comment = "目的IP所在城市")
    private String dstIpCity;

    @TableField(name = "category", comment = "威胁类别")
    private String category;
    @TableField(name = "kill_chain", comment = "事件攻击链位置")
    private String killChain;
    @TableField(name = "severity_cn", comment = "威胁严重性中文")
    private String severityCn;
    @TableField(name = "severity", type = "int", comment = "威胁严重性")
    private int severity;

    @TableField(name = "atd_desc", comment = "威胁描述")
    private String atdDesc;

    @TableField(name = "conn_src_mac", comment = "源主机Mac地址")
    private String connSrcMac;
    @TableField(name = "conn_duration", comment = "持续时间")
    private String connDuration;
    @TableField(name = "conn_dst_mac", comment = "目的主机Mac地址")
    private String connDstMac;

    @TableField(name = "payload", comment = "攻击/异常载荷")
    private String payload;

    @TableField(name = "family", comment = "威胁所在家族")
    private String family;
    @TableField(name = "dns_query", comment = "查询内容")
    private String dnsQuery;

    @TableField(name = "count", type = "int", comment = "事件发生数量")
    private int count;

    @TableField(name = "begin_time", type = "timestamp", comment = "事件开始时间")
    private Date beginTime;

    @TableField(name = "stop_time", type = "timestamp", comment = "事件结束时间")
    private Date stopTime;

    @TableField(name = "create_time", type = "timestamp", comment = "创建时间")
    private Date createTime;

    @TableField(name = "proto", comment = "传输层协议")
    private String proto;
    @TableField(name = "app_proto", comment = "应用层协议")
    private String appProto;
    @TableField(name = "http_host", comment = "主机名")
    private String httpHost;

    //攻击方
    @TableField(name = "attack_ip", comment = "攻击方ip")
    private String attackIp;
    @TableField(name = "attack_port", comment = "攻击方端口")
    private String attackPort;
    @TableField(name = "attack_country", comment = "攻击方国家")
    private String attackCountry;
    @TableField(name = "attack_province", comment = "攻击方省份")
    private String attackProvince;
    @TableField(name = "attack_city", comment = "攻击方城市")
    private String attackCity;
    @TableField(name = "attack_corpname", comment = "攻击方企业名称")
    private String attackCorpName;
    @TableField(name = "attack_industry", comment = "攻击方行业分类")
    private String attackIndustry;
    @TableField(name = "attack_corp_type", comment = "攻击方企业性质")
    private String attackCorpType;

    //被攻击方
    @TableField(name = "attacked_ip", comment = "被攻击方ip")
    private String attackedIp;
    @TableField(name = "attacked_port", comment = "被攻击方端口")
    private String attackedPort;
    @TableField(name = "attacked_country", comment = "被攻击方国家")
    private String attackedCountry;
    @TableField(name = "attacked_province", comment = "被攻击方省份")
    private String attackedProvince;
    @TableField(name = "attacked_city", comment = "被攻击方城市名称")
    private String attackedCity;
    @TableField(name = "attacked_corpname", comment = "被攻击方企业名称")
    private String attackedCorpName;
    @TableField(name = "attacked_industry", comment = "被攻击方行业分类")
    private String attackedIndustry;
    @TableField(name = "attacked_corp_type", comment = "被攻击方企业性质")
    private String attackedCorpType;

    public String generateKey() {
        String timeTag = "";
        if (beginTime != null) {
            timeTag = DateUtils.formatDate(beginTime, "yyyy-MM-dd");
        }
        return "SyslogNotice{" +
                "timeTag='" + timeTag + "\'" +
                "category=" + category + "\'" +
                ",srcIp='" + srcIp + "\'" +
                ",srcPort='" + srcPort + "\'" +
                ",dstIp='" + dstIp + "\'" +
                ",dstPort='" + dstPort + "\'" +
                '}';
    }
}
