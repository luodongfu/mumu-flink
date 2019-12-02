package com.lovecws.mumu.flink.common.model.jmr;

/**
 * @program: jmr
 * @description: 僵木蠕样本描述文件
 * @author: 甘亮
 * @create: 2018-11-19 13:28
 **/
public class JmrSampleEventModel extends JmrEventModel {

    private String orifilename;//样本原始文件名
    private String sample_content;//样本字节数组

    public String getOrifilename() {
        return orifilename;
    }

    public void setOrifilename(String orifilename) {
        this.orifilename = orifilename;
    }

    public String getSample_content() {
        return sample_content;
    }

    public void setSample_content(String sample_content) {
        this.sample_content = sample_content;
    }
}
