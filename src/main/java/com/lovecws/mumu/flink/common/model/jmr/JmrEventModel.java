package com.lovecws.mumu.flink.common.model.jmr;

public class JmrEventModel extends EventModel {

    private String engine_type;//引擎类型,引擎类型由平台统一管理，用来识别不同引擎
    private String dvc_event_class_id;//告警命中的规则ID，与特征库中相关联。

    private String feature_lib_version;//特征库版本号
    private String feature_lib;//特征库
    private String event_feature;//恶意行为类型 ip、URL、域名、MD5、报文特征

    private String malware_behavior_type;//事件类型(僵尸木马控制端、被控端、蠕虫)
    private String event_type;//事件类型(僵尸木马控制端、被控端、蠕虫)
    private String event_name;//事件名称，视扫描到的事件而定。如：菜刀上线1.3
    private String event_sub_type;//事件子类型(DoS攻击木马)
    private String malware_type;//恶意软件类型(如Win32/Trojan.97a)

    private String malware_filename;//恶意软件名
    private String description;//主机受控/传播事件描述
    private String malware_md5;//恶意样本MD5

    private String suspected_file_cap;//疑似文件容量。
    private String event;//匹配流量内容的摘要

    private int duration;//持续时间，单位秒。
    private int pckcnt;//命中规则的流的包数
    private int bytecnt;//命中规则的流的总流量数，单位字节

    private String vendorid;//厂商ID
    private String devid;//设备ID（对于引擎设备的唯一标识）

    private String url;//HTTP协议为访问的URL，其他协议为空
    private String domain_name;//域名


    public String getEngine_type() {
        return engine_type;
    }

    public void setEngine_type(String engine_type) {
        this.engine_type = engine_type;
    }

    public String getDvc_event_class_id() {
        return dvc_event_class_id;
    }

    public void setDvc_event_class_id(String dvc_event_class_id) {
        this.dvc_event_class_id = dvc_event_class_id;
    }

    public String getFeature_lib_version() {
        return feature_lib_version;
    }

    public void setFeature_lib_version(String feature_lib_version) {
        this.feature_lib_version = feature_lib_version;
    }

    public String getFeature_lib() {
        return feature_lib;
    }

    public void setFeature_lib(String feature_lib) {
        this.feature_lib = feature_lib;
    }

    public String getEvent_feature() {
        return event_feature;
    }

    public void setEvent_feature(String event_feature) {
        this.event_feature = event_feature;
    }

    public String getMalware_behavior_type() {
        return malware_behavior_type;
    }

    public void setMalware_behavior_type(String malware_behavior_type) {
        this.malware_behavior_type = malware_behavior_type;
    }

    public String getEvent_type() {
        return event_type;
    }

    public void setEvent_type(String event_type) {
        this.event_type = event_type;
    }

    public String getEvent_name() {
        return event_name;
    }

    public void setEvent_name(String event_name) {
        this.event_name = event_name;
    }

    public String getEvent_sub_type() {
        return event_sub_type;
    }

    public void setEvent_sub_type(String event_sub_type) {
        this.event_sub_type = event_sub_type;
    }

    public String getMalware_type() {
        return malware_type;
    }

    public void setMalware_type(String malware_type) {
        this.malware_type = malware_type;
    }

    public String getMalware_filename() {
        return malware_filename;
    }

    public void setMalware_filename(String malware_filename) {
        this.malware_filename = malware_filename;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getMalware_md5() {
        return malware_md5;
    }

    public void setMalware_md5(String malware_md5) {
        this.malware_md5 = malware_md5;
    }

    public String getSuspected_file_cap() {
        return suspected_file_cap;
    }

    public void setSuspected_file_cap(String suspected_file_cap) {
        this.suspected_file_cap = suspected_file_cap;
    }

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public int getDuration() {
        return duration;
    }

    public void setDuration(int duration) {
        this.duration = duration;
    }

    public int getPckcnt() {
        return pckcnt;
    }

    public void setPckcnt(int pckcnt) {
        this.pckcnt = pckcnt;
    }

    public int getBytecnt() {
        return bytecnt;
    }

    public void setBytecnt(int bytecnt) {
        this.bytecnt = bytecnt;
    }

    public String getVendorid() {
        return vendorid;
    }

    public void setVendorid(String vendorid) {
        this.vendorid = vendorid;
    }

    public String getDevid() {
        return devid;
    }

    public void setDevid(String devid) {
        this.devid = devid;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDomain_name() {
        return domain_name;
    }

    public void setDomain_name(String domain_name) {
        this.domain_name = domain_name;
    }
}
