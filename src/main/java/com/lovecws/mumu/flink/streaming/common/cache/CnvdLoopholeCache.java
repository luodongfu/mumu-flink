package com.lovecws.mumu.flink.streaming.common.cache;

import com.lovecws.mumu.flink.common.config.ConfigProperties;
import com.lovecws.mumu.flink.common.model.loophole.CnvdModel;
import com.lovecws.mumu.flink.common.redis.JedisUtil;
import com.lovecws.mumu.flink.common.serialize.SerializeUtil;
import com.lovecws.mumu.flink.common.service.loophole.CnvdService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.JedisCommands;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: trunk
 * @description: cnvd漏洞信息缓存
 * @author: 甘亮
 * @create: 2019-09-19 18:34
 **/
@Slf4j
public class CnvdLoopholeCache implements Serializable {

    private CnvdService cnvdService=new CnvdService();

    private String prefix;
    private int expire;

    public CnvdLoopholeCache() {
        prefix = ConfigProperties.getString("streaming.defaults.cache.cnvd.prefix", "INDUSTRY_DATAPROCESSING_CNVD");
        expire = ConfigProperties.getInteger("streaming.defaults.cache.cnvd.expire", 86440);
    }

    /**
     * 缓存产品对应的漏洞列表
     *
     * @param reflectProduct 产品名称
     * @return
     */
    public List<CnvdModel> queryCnvdRefectProduct(String reflectProduct) {
        List<CnvdModel> cnvdModels = new ArrayList<>();
        if (StringUtils.isEmpty(reflectProduct)) return cnvdModels;

        JedisCommands jedis = JedisUtil.getJedis();
        try {
            Object cnvdModelsObj = jedis.hget(prefix, reflectProduct);
            if (cnvdModelsObj == null) {
                Boolean exists = jedis.exists(prefix);
                if (exists == null || !exists) {
                    jedis.hset(prefix, "temp", "");
                    //缓存数据
                    jedis.expire(prefix, expire);
                }

                cnvdModels = cnvdService.queryCnvdRefectProduct(reflectProduct);
                byte[] serialize = SerializeUtil.serialize(cnvdModels, SerializeUtil.HESSIAN2_SERIALIZE);
                jedis.hset(prefix, reflectProduct, Base64.encodeBase64String(serialize));
            } else {
                Base64.decodeBase64(cnvdModelsObj.toString());
                cnvdModels = (List<CnvdModel>) SerializeUtil.deserialize(Base64.decodeBase64(cnvdModelsObj.toString()), SerializeUtil.HESSIAN2_SERIALIZE);
            }
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage());
        } finally {
            JedisUtil.close(jedis);
        }

        return cnvdModels;
    }
}
