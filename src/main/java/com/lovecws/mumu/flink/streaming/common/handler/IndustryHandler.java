package com.lovecws.mumu.flink.streaming.common.handler;

import com.lovecws.mumu.flink.common.annotation.ModularAnnotation;
import com.lovecws.mumu.flink.common.config.ConfigProperties;
import com.lovecws.mumu.flink.common.redis.JedisUtil;
import com.lovecws.mumu.flink.common.util.HttpClientUtil;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.JedisCommands;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 根据企业名称获取行业分类信息
 */
@Slf4j
@ModularAnnotation(type = "handler", name = "industry")
public class IndustryHandler implements Serializable {

    private String url;
    private String authorization;
    //缓存前缀 默认 INDUSTRY_DATAPROCESSING_INDUSTRY
    private String prefix;
    //缓存过期时间 默认一天
    private int expire;
    //分桶数量
    private int bucket;

    public IndustryHandler() {
        url = ConfigProperties.getString("streaming.defaults.industry.url", "localhost");
        authorization = ConfigProperties.getString("streaming.defaults.industry.authorization", "bearer 1c1ce451-6940-4c70-857b-cb35b4f29f9f");
        prefix = ConfigProperties.getString("streaming.defaults.industry.prefix", "INDUSTRY_DATAPROCESSING_INDUSTRY");
        expire = ConfigProperties.getInteger("streaming.defaults.industry.expire", 86440);
        bucket = ConfigProperties.getInteger("streaming.defaults.industry.bucket", 24);
    }

    /**
     * 获取到公司企业名称
     *
     * @param companyName 企业名称
     * @return
     */
    public Map<String, Object> handler(String companyName) {
        if (StringUtils.isEmpty(companyName)) return new HashMap<>();
        JedisCommands jedis = JedisUtil.getJedis();
        Map<String, Object> cacheMap = new HashMap<>();
        //计算redis的hash名称
        String hashKey = prefix;
        if (bucket > 0) {
            long mode = Math.abs(companyName.hashCode()) % bucket;
            hashKey = prefix + "_" + String.valueOf(mode);
        }
        Object industyInfo = null;
        try {
            //从缓存获取数据
            industyInfo = jedis.hget(hashKey, companyName);
            if (industyInfo != null && StringUtils.isNotEmpty(industyInfo.toString())) {
                cacheMap = JSON.parseObject(industyInfo.toString(), Map.class);
            } else {
                cacheMap = getIndustry(url, authorization, companyName);

                Boolean exists = jedis.exists(hashKey);
                jedis.hset(hashKey, companyName, JSON.toJSONString(cacheMap));
                //缓存数据
                if (!exists) jedis.expire(hashKey, expire);
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage() + "[" + industyInfo + "]");
        } finally {
            JedisUtil.close(jedis);
        }
        return cacheMap;
    }

    /**
     * 获取企业行业分类信息
     *
     * @param url
     * @param authorization
     * @param companyName
     * @return 行业分类，调整为取category_smallname
     * {
     * "industrial_code": "440301113443997",
     * "company_type": 32767,
     * "creditcode": null,
     * "legal_person_type": 1,
     * "category_name": "信息传输、软件和信息技术服务业",
     * "third_format": null,
     * "legal_person_id": 518939,
     * "province": "广东",
     * "insured_time": null,
     * "reg_location": "深圳市前海深港合作区前湾一路1号A栋201室（入驻深圳市前海商务秘书有限公司）（经营场所：深圳市南山区粤海街道深圳湾科技生态园二区9栋B座920）",
     * "id": "8832c2f01a663d4ae0531a861fac75b2",
     * "create_date": "2019-05-06 19:55:57",
     * "approval_date": "2019-01-08 00:00:00",
     * "town": "NULL",
     * "category_code": null,
     * "is_has": null,
     * "category_smallname": null,
     * "business_scope": "云计算技术的开发与销售；计算机软件、硬件产品的设计、技术开发和销售；计算机系统集成；网络技术开发与销售；通讯及电子产品的技术开发、销售；云计算平台管理；信息咨询（不含限制项目）；物业管理；国内贸易、经营进出口业务（不含专营、专控、专卖商品）。（以上各项涉及法律、行政法规、国务院决定禁止的项目除外，限制的项目须取得许可后方可经营）互联网数据中心业务、互联网接入服务、互联网资源协作服务",
     * "org_approved_institute": " ",
     * "phone": null,
     * "parent_id": null,
     * "company_name": "深圳前海小鸟云计算有限公司",
     * "company_aliasname": null,
     * "regis_authority": "南山局",
     * "is_oncloud": 0,
     * "org_code": "349681953",
     * "flag": null,
     * "finorg_type": -1,
     * "is_die": null,
     * "city": "深圳市",
     * "second_format": null,
     * "category_smallcode": null,
     * "genxin": null,
     * "company_enname": null,
     * "actual_capital": null,
     * "to_date": null,
     * "street": null,
     * "company_status": "存续（在营、开业、在册）",
     * "email": null,
     * "address": null,
     * "from_date": "2015-07-22 00:00:00",
     * "registered_capital": "2400.000000万人民币",
     * "company_org_type": "有限责任公司",
     * "url": null,
     * "estb_date": "2015-07-22 00:00:00",
     * "service_type": "000",
     * "person_scale": null,
     * "insured_nums": null,
     * "legal_person_name": "彭荣涛",
     * "parent_companyname": null,
     * "tycurl": null,
     * "modify_date": "2019-05-06 19:55:57",
     * "base": "gd"
     * }
     */
    public Map<String, Object> getIndustry(String url, String authorization, String companyName) {
        Map<String, Object> headerMap = new HashMap<>();
        if (StringUtils.isEmpty(companyName)) return new HashMap<String, Object>();
        headerMap.put("Authorization", authorization);

        String resp = null;
        try {
            resp = HttpClientUtil.post(url + "?companyName=" + URLEncoder.encode(companyName, "UTF-8"), new HashMap<>(), headerMap);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        if (resp == null) return new HashMap<>();

        Map map = JSON.parseObject(resp, Map.class);
        List<Map> results = (List<Map>) map.get("result");
        return results == null || results.size() == 0 ? new HashMap<>() : results.get(0);
    }
}
