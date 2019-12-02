package com.lovecws.mumu.flink.common.serialize;

import com.alibaba.fastjson.JSON;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 序列化工具类
 * @date 2017-11-22 14:17
 */
public class SerializeUtil {

    //普通的json序列化，但是在反序列化的时候会出现类型转换错误
    public static final String JSON_SERIALIZE = "JSON";

    public static final String HESSIAN_SERIALIZE = "HESSIAN";
    public static final String HESSIAN2_SERIALIZE = "HESSIAN2";
    public static final String HESSIANSERIALIZER = "HESSIANSERIALIZER";

    public static final String MARSHALLING_SERIALIZE = "MARSHALLING";
    public static final String JAVA_SERIALIZE = "JAVA";

    /**
     * 反序列化
     *
     * @param cacheData 缓存数据
     * @param serializeType 序列化类型
     * @return
     */
    public static Object deserialize(byte[] cacheData, String serializeType) {
        if (cacheData == null) return null;
        Object deserialize = null;
        if (JSON_SERIALIZE.equalsIgnoreCase(serializeType)) {
            deserialize = JSON.parse(cacheData);
        } else if (HESSIAN_SERIALIZE.equalsIgnoreCase(serializeType)) {
            deserialize = HessianSerializeUtil.deserialize(cacheData, HessianSerializeUtil.HessianType.HESSIAN);
        } else if (HESSIAN2_SERIALIZE.equalsIgnoreCase(serializeType)) {
            deserialize = HessianSerializeUtil.deserialize(cacheData, HessianSerializeUtil.HessianType.HESSIAN2);
        } else if (HESSIANSERIALIZER.equalsIgnoreCase(serializeType)) {
            deserialize = HessianSerializeUtil.deserialize(cacheData, HessianSerializeUtil.HessianType.HessianSerializer);
        } else if (MARSHALLING_SERIALIZE.equalsIgnoreCase(serializeType)) {
            deserialize = MarshallingSerializeUtil.deserialize(cacheData);
        } else if (JAVA_SERIALIZE.equalsIgnoreCase(serializeType)) {
            deserialize = JavaSerializeUtil.deserialize(cacheData);
        } else {
            deserialize = JavaSerializeUtil.deserialize(cacheData);
        }
        return deserialize;
    }


    /**
     * 序列化
     *
     * @param cacheData 缓存数据
     * @param serializeType 序列化类型
     * @return
     */
    public static byte[] serialize(Object cacheData, String serializeType) {
        if (cacheData == null) return null;
        if (serializeType == null) serializeType = HESSIAN2_SERIALIZE;
        byte[] serialize = null;
        if (JSON_SERIALIZE.equalsIgnoreCase(serializeType)) {
            serialize = JSON.toJSONBytes(cacheData);
        } else if (HESSIAN_SERIALIZE.equalsIgnoreCase(serializeType)) {
            serialize = HessianSerializeUtil.serialize(cacheData, HessianSerializeUtil.HessianType.HESSIAN);
        } else if (HESSIAN2_SERIALIZE.equalsIgnoreCase(serializeType)) {
            serialize = HessianSerializeUtil.serialize(cacheData, HessianSerializeUtil.HessianType.HESSIAN2);
        } else if (HESSIANSERIALIZER.equalsIgnoreCase(serializeType)) {
            serialize = HessianSerializeUtil.serialize(cacheData, HessianSerializeUtil.HessianType.HessianSerializer);
        } else if (MARSHALLING_SERIALIZE.equalsIgnoreCase(serializeType)) {
            serialize = MarshallingSerializeUtil.serialize(cacheData);
        } else if (JAVA_SERIALIZE.equalsIgnoreCase(serializeType)) {
            serialize = JavaSerializeUtil.serialize(cacheData);
        } else {
            serialize = JavaSerializeUtil.serialize(cacheData);
        }
        return serialize;
    }
}
