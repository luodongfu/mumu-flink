package com.lovecws.mumu.flink.common.util;

/**
 * @program: mumu-flink
 * @description: 字符串进制转换工具类
 * @author: 甘亮
 * @create: 2019-07-16 16:39
 **/
public class StringRadisUtil {

    /**
     * 字符串转换成16进制
     *
     * @param str 字符串
     * @return
     */
    public static String str2HexStr(String str) {
        char[] chars = "0123456789ABCDEF".toCharArray();
        StringBuilder sb = new StringBuilder("");
        byte[] bs = str.getBytes();
        int bit;
        for (int i = 0; i < bs.length; i++) {
            bit = (bs[i] & 0x0f0) >> 4;
            sb.append(chars[bit]);
            bit = bs[i] & 0x0f;
            sb.append(chars[bit]);
            // sb.append(' ');
        }
        return sb.toString().trim();
    }

    /**
     * 16进制字符转换为转义符
     *
     * @param hexStr 16进制字符
     * @return
     */
    private static String hexStr2Str(String hexStr) {
        String str = "0123456789ABCDEF";
        char[] hexs = hexStr.toCharArray();
        byte[] bytes = new byte[hexStr.length() / 2];
        int n;
        for (int i = 0; i < bytes.length; i++) {
            n = str.indexOf(hexs[2 * i]) * 16;
            n += str.indexOf(hexs[2 * i + 1]);
            bytes[i] = (byte) (n & 0xff);
        }
        return new String(bytes);
    }

    /**
     * 将转义字符转化为16进制
     *
     * @param str         字符串
     * @param escapeChars 转义字符集合
     * @param reverse     true:将特殊字符转换为16进制，false:将16进制转换为转义字符
     */
    public static String transform(String str, String[] escapeChars, boolean reverse) {
        if (str == null || escapeChars == null) return str;
        String cvalue = str;
        for (String escapeChar : escapeChars) {
            if (reverse) {
                cvalue = cvalue.replaceAll(escapeChar, "%" + str2HexStr(escapeChar));
                //cvalue = cvalue.replaceAll(escapeChar, URLEncoder.encode(escapeChar));
            } else {
                cvalue = cvalue.replaceAll("%" + str2HexStr(escapeChar), escapeChar);
                //cvalue = cvalue.replaceAll(URLEncoder.encode(escapeChar), escapeChar);
            }
        }
        return cvalue;
    }
}
