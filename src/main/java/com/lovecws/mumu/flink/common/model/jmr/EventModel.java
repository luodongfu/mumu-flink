package com.lovecws.mumu.flink.common.model.jmr;

import java.io.Serializable;
import java.util.Date;

public class EventModel implements Serializable {

    private String key;//es索引id

    private String protocol;//协议类型.如HTTP、FTP、SMTP、POP

    private String identification;//源IP为控制端用“0”标识，源IP为受控端用“1”标识，源IP为传播端用“2”标识，源IP为感染端用“3”标识。

    private String src_network_mode;//IPV4用“0”表示，IPV6用“1”表示。
    private String src_ip;//源ip
    private long src_ip_value;//源ip十进制值
    private String src_ip_country;//源ip所属国家
    private String src_ip_province;//源ip所属省份
    private String src_ip_city;//源ip所属城市
    private String src_ip_operator;//源ip所属运营商
    private String src_ip_longitude;//源IP经度
    private String src_ip_latitude;//源IP维度
    private int src_port;//源端口
    private String src_host;//源ip+源端口,定位到源主机

    private String dest_network_mode;//IPV4用“0”表示，IPV6用“1”表示。
    private String dest_ip;//目的ip
    private long dest_ip_value;//目的ip十进制值
    private String dest_ip_country;//目的ip所属国家
    private String dest_ip_province;//目的ip所属省份
    private String dest_ip_city;//目的ip所属城市
    private String dest_ip_operator;//目的ip所属运营商
    private String dest_ip_longitude;//目的IP经度
    private String dest_ip_latitude;//目的IP维度
    private int dest_port;//目标端口号
    private String dest_host;//目的ip+目的端口dest，定位到目的主机
    private String event_host;//src_key+dest_key组合而成的键

    private Date event_time;//时间发生时间
    private Date create_time;//时间创建时间

    private String province_id;//上报的省份id
    private String province_name;//省份名称
    private String operator;//运营商

    private int attack_num;//事件在固定的事件内的攻击次数

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getIdentification() {
        return identification;
    }

    public void setIdentification(String identification) {
        this.identification = identification;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getSrc_network_mode() {
        return src_network_mode;
    }

    public void setSrc_network_mode(String src_network_mode) {
        this.src_network_mode = src_network_mode;
    }

    public String getSrc_ip() {
        return src_ip;
    }

    public void setSrc_ip(String src_ip) {
        this.src_ip = src_ip;
    }

    public long getSrc_ip_value() {
        return src_ip_value;
    }

    public void setSrc_ip_value(long src_ip_value) {
        this.src_ip_value = src_ip_value;
    }

    public String getSrc_ip_country() {
        return src_ip_country;
    }

    public void setSrc_ip_country(String src_ip_country) {
        this.src_ip_country = src_ip_country;
    }

    public String getSrc_ip_province() {
        return src_ip_province;
    }

    public void setSrc_ip_province(String src_ip_province) {
        this.src_ip_province = src_ip_province;
    }

    public String getSrc_ip_city() {
        return src_ip_city;
    }

    public void setSrc_ip_city(String src_ip_city) {
        this.src_ip_city = src_ip_city;
    }

    public String getSrc_ip_operator() {
        return src_ip_operator;
    }

    public void setSrc_ip_operator(String src_ip_operator) {
        this.src_ip_operator = src_ip_operator;
    }

    public String getSrc_ip_longitude() {
        return src_ip_longitude;
    }

    public void setSrc_ip_longitude(String src_ip_longitude) {
        this.src_ip_longitude = src_ip_longitude;
    }

    public String getSrc_ip_latitude() {
        return src_ip_latitude;
    }

    public void setSrc_ip_latitude(String src_ip_latitude) {
        this.src_ip_latitude = src_ip_latitude;
    }

    public int getSrc_port() {
        return src_port;
    }

    public void setSrc_port(int src_port) {
        this.src_port = src_port;
    }

    public String getDest_network_mode() {
        return dest_network_mode;
    }

    public void setDest_network_mode(String dest_network_mode) {
        this.dest_network_mode = dest_network_mode;
    }

    public String getDest_ip() {
        return dest_ip;
    }

    public void setDest_ip(String dest_ip) {
        this.dest_ip = dest_ip;
    }

    public long getDest_ip_value() {
        return dest_ip_value;
    }

    public void setDest_ip_value(long dest_ip_value) {
        this.dest_ip_value = dest_ip_value;
    }

    public String getDest_ip_country() {
        return dest_ip_country;
    }

    public void setDest_ip_country(String dest_ip_country) {
        this.dest_ip_country = dest_ip_country;
    }

    public String getDest_ip_province() {
        return dest_ip_province;
    }

    public void setDest_ip_province(String dest_ip_province) {
        this.dest_ip_province = dest_ip_province;
    }

    public String getDest_ip_city() {
        return dest_ip_city;
    }

    public void setDest_ip_city(String dest_ip_city) {
        this.dest_ip_city = dest_ip_city;
    }

    public String getDest_ip_operator() {
        return dest_ip_operator;
    }

    public void setDest_ip_operator(String dest_ip_operator) {
        this.dest_ip_operator = dest_ip_operator;
    }

    public String getDest_ip_longitude() {
        return dest_ip_longitude;
    }

    public void setDest_ip_longitude(String dest_ip_longitude) {
        this.dest_ip_longitude = dest_ip_longitude;
    }

    public String getDest_ip_latitude() {
        return dest_ip_latitude;
    }

    public void setDest_ip_latitude(String dest_ip_latitude) {
        this.dest_ip_latitude = dest_ip_latitude;
    }

    public int getDest_port() {
        return dest_port;
    }

    public void setDest_port(int dest_port) {
        this.dest_port = dest_port;
    }

    public Date getEvent_time() {
        return event_time;
    }

    public void setEvent_time(Date event_time) {
        this.event_time = event_time;
    }

    public Date getCreate_time() {
        return create_time;
    }

    public void setCreate_time(Date create_time) {
        this.create_time = create_time;
    }

    public String getProvince_id() {
        return province_id;
    }

    public void setProvince_id(String province_id) {
        this.province_id = province_id;
    }

    public String getProvince_name() {
        return province_name;
    }

    public void setProvince_name(String province_name) {
        this.province_name = province_name;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public int getAttack_num() {
        return attack_num;
    }

    public void setAttack_num(int attack_num) {
        this.attack_num = attack_num;
    }

    public String getSrc_host() {
        return src_host;
    }

    public void setSrc_host(String src_host) {
        this.src_host = src_host;
    }

    public String getDest_host() {
        return dest_host;
    }

    public void setDest_host(String dest_host) {
        this.dest_host = dest_host;
    }

    public String getEvent_host() {
        return event_host;
    }

    public void setEvent_host(String event_host) {
        this.event_host = event_host;
    }

    //非持续化字段
    private String line;//读取的数据行

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        this.line = line;
    }
}
