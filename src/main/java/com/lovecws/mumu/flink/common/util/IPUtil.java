package com.lovecws.mumu.flink.common.util;

import org.apache.commons.lang3.StringUtils;

import java.math.BigInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IPUtil {

    public static final Pattern ipv6Reg = Pattern.compile(
            "^\\s*((([0-9A-Fa-f]{1,4}:){7}([0-9A-Fa-f]{1,4}|:))|(([0-9A-Fa-f]{1,4}:){6}(:[0-9A-Fa-f]{1,4}|((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){5}(((:[0-9A-Fa-f]{1,4}){1,2})|:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){4}(((:[0-9A-Fa-f]{1,4}){1,3})|((:[0-9A-Fa-f]{1,4})?:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){3}(((:[0-9A-Fa-f]{1,4}){1,4})|((:[0-9A-Fa-f]{1,4}){0,2}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){2}(((:[0-9A-Fa-f]{1,4}){1,5})|((:[0-9A-Fa-f]{1,4}){0,3}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){1}(((:[0-9A-Fa-f]{1,4}){1,6})|((:[0-9A-Fa-f]{1,4}){0,4}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(:(((:[0-9A-Fa-f]{1,4}){1,7})|((:[0-9A-Fa-f]{1,4}){0,5}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:)))(%.+)?\\s*$");
    // IPV4正则表达式
    private static final Pattern ipv4Reg = Pattern
            .compile("^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$");
    private static final Pattern ipv4Reg1 = Pattern.compile("^0[0-9]{1,2}\\.|\\.0[0-9]{1,2}");// ip的数字不能以0开头，部里的要求
    private static final int IPV6Length = 8; // IPV6地址的分段
    private static final int IPV4Length = 4; // IPV6地址分段
    private static final int IPV4ParmLength = 2; // 一个IPV4分段占的长度
    private static final int IPV6ParmLength = 4; // 一个IPV6分段占的长

    public static long ipToLong(String ipString) {
        if (StringUtils.isEmpty(ipString) || "*".equals(ipString)) {
            return 0L;
        }
        if ("ipv6".equalsIgnoreCase(IPUtil.ipType(ipString))) {
            return IPUtil.ipv6toInt(ipString).intValue();
        }
        long result = 0;
        java.util.StringTokenizer token = new java.util.StringTokenizer(ipString, ".");
        if (token.hasMoreTokens()) result += Long.parseLong(token.nextToken()) << 24;
        if (token.hasMoreTokens()) result += Long.parseLong(token.nextToken()) << 16;
        if (token.hasMoreTokens()) result += Long.parseLong(token.nextToken()) << 8;
        if (token.hasMoreTokens()) result += Long.parseLong(token.nextToken());
        return Math.abs(result);
    }

    public static String longToIp(long ipLong) {
        StringBuilder sb = new StringBuilder();
        sb.append(ipLong >>> 24);
        sb.append(".");
        sb.append(String.valueOf((ipLong & 0x00FFFFFF) >>> 16));
        sb.append(".");
        sb.append(String.valueOf((ipLong & 0x0000FFFF) >>> 8));
        sb.append(".");
        sb.append(String.valueOf(ipLong & 0x000000FF));
        return sb.toString();
    }

    /**
     * verifyIpStandard:判断是否是IP地址. <br/>
     *
     * @param ip
     * @return
     * @author Alex
     * @since JDK 1.6
     */
    public static boolean verifyIpStandard(String ip) {
        String ipV4verify = "^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])\\.(\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\.(\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\.(\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)$";
        Pattern pattern = Pattern.compile(ipV4verify);
        Matcher matcher = pattern.matcher(ip);
        if (matcher.matches()) {
            return true;
        }
        Matcher matcherV6 = ipv6Reg.matcher(ip);
        return matcherV6.matches();
    }

    /**
     * 获取IP类型( 1 - IPV4 , 2 - IPV6)
     */
    public static String ipType(String ip) {
        if (ip == null) {
            return null;
        }
        boolean isValid = ipv4Reg.matcher(ip).matches() && !ipv4Reg1.matcher(ip).find();
        if (!isValid) {
            isValid = ipv6Reg.matcher(ip).matches();
            if (isValid) {
                return "ipv6";
            } else {
                return "ipv4";
            }
        } else {
            return "ipv4";
        }
    }

    /**
     * ipv6toInt:(将IPV6 的信息转换成十进制). <br/>
     *
     * @param ipv6
     * @return
     * @author quanli
     * @since JDK 1.6
     */
    public static BigInteger ipv6toInt(String ipv6) {
        String result = buildKey(ipv6);
//		System.out.println(result);
        String ipv6string = "";
        for (int i = 0; i < result.length(); i = i + 4) {

            ipv6string += org.apache.commons.lang.StringUtils.substring(result, i, i + 4);
            ipv6string += ':';
        }
        ipv6string = org.apache.commons.lang.StringUtils.substring(ipv6string, 0, ipv6string.length() - 1);
        String[] str = ipv6string.split(":");
        BigInteger big = BigInteger.ZERO;
        for (int i = 0; i < str.length; i++) {
            // ::1
            if (str[i].isEmpty()) {
                str[i] = "0";
            }
            big = big.add(BigInteger.valueOf(Long.valueOf(str[i], 16)).shiftLeft(16 * (str.length - i - 1)));
        }
        return big;
    }

    /**
     * IPV6、IPV4转化为十六进制串
     *
     * @param ipAddress
     * @return
     */
    private static String buildKey(String ipAddress) {
        String Key = "";
        // ipv4标识 。判断是否是ipv4地址
        int dotFlag = ipAddress.indexOf(".");
        // ipv6标识 。判断是否是ipv6地址
        int colonFlag = ipAddress.indexOf(":");
        // ipv6标识 。判断是否是简写的ipv6地址
        int dColonFlag = ipAddress.indexOf("::");
        // 将v6或v4的分隔符用&代替
        ipAddress = ipAddress.replace(".", "&");
        ipAddress = ipAddress.replace(":", "&");
        // ipv4 address。将ipv4地址转换成16进制的形式
        if (dotFlag != -1 && colonFlag == -1) {
            String[] arr = ipAddress.split("&");
            // 1、 ipv4转ipv6，前4组数补0或f
            for (int i = 0; i < IPV6Length - IPV4ParmLength; i++) {
                // 根据v4转v6的形式，除第4组数补ffff外，前3组数补0000
                if (i == IPV6Length - IPV4ParmLength - 1) {
                    Key += "ffff";
                } else {
                    Key += "0000";
                }
            }
            // 2、将ipv4地址转成16进制
            for (int j = 0; j < IPV4Length; j++) {
                // 1)将每组ipv4地址转换成16进制
                arr[j] = Integer.toHexString(Integer.parseInt(arr[j]));
                // 2) 位数不足补0，ipv4地址中一组可转换成一个十六进制，两组数即可标识ipv6中的一组，v6中的一组数不足4位补0
                for (int k = 0; k < (IPV4ParmLength - arr[j].length()); k++) {
                    Key += "0";
                }
                Key += arr[j];
            }
        }
        // Mixed address with ipv4 and ipv6。将v4与v6的混合地址转换成16进制的形式
        if (dotFlag != -1 && colonFlag != -1 && dColonFlag == -1) {
            String[] arr = ipAddress.split("&");

            for (int i = 0; i < IPV6Length - IPV4ParmLength; i++) {
                // 将ip地址中每组不足4位的补0
                for (int k = 0; k < (IPV6ParmLength - arr[i].length()); k++) {
                    Key += "0";
                }
                Key += arr[i];
            }

            for (int j = 6; j < 10; j++) {
                arr[j] = Integer.toHexString(Integer.parseInt(arr[j]));
                for (int k = 0; k < (IPV4ParmLength - arr[j].length()); k++) {
                    Key += "0";
                }
                Key += arr[j];
            }
        }
        // Mixed address with ipv4 and ipv6,and there are more than one
        // '0'。将v4与v6的混合地址(如::32:dc:192.168.62.174)转换成16进制的形式
        // address param
        if (dColonFlag != -1 && dotFlag != -1) {
            String[] arr = ipAddress.split("&");
            // 存放16进制的形式
            String[] arrParams = new String[IPV6Length + IPV4ParmLength];
            int indexFlag = 0;
            int pFlag = 0;
            // 1、将简写的ip地址补0
            // 如果ip地址中前面部分采用简写，做如下处理
            if ("".equals(arr[0])) {
                // 1)如果ip地址采用简写形式，不足位置补0，存放到arrParams中
                for (int j = 0; j < (IPV6Length + IPV4ParmLength - (arr.length - 2)); j++) {
                    arrParams[j] = "0000";
                    indexFlag++;
                }
                // 2)将已有值的部分(如32:dc:192.168.62.174)存放到arrParams中
                for (int i = 2; i < arr.length; i++) {
                    arrParams[indexFlag] = arr[i];
                    indexFlag++;
                }
            } else {
                for (int i = 0; i < arr.length; i++) {
                    if ("".equals(arr[i])) {
                        for (int j = 0; j < (IPV6Length + IPV4ParmLength
                                - arr.length + 1); j++) {
                            arrParams[indexFlag] = "0000";
                            indexFlag++;
                        }
                    } else {
                        arrParams[indexFlag] = arr[i];
                        indexFlag++;
                    }
                }
            }
            // 2、ip(去除ipv4的部分)中采用4位十六进制数表示一组数，将不足4位的十六进制数补0
            for (int i = 0; i < IPV6Length - IPV4ParmLength; i++) {
                // 如果arrParams[i]组数据不足4位，前补0
                for (int k = 0; k < (IPV6ParmLength - arrParams[i].length()); k++) {
                    Key += "0";
                }
                Key += arrParams[i];
                // pFlag用于标识位置，主要用来标识ipv4地址的起始位
                pFlag++;
            }
            // 3、将ipv4地址转成16进制
            for (int j = 0; j < IPV4Length; j++) {
                // 1)将每组ipv4地址转换成16进制
                arrParams[pFlag] = Integer.toHexString(Integer
                        .parseInt(arrParams[pFlag]));
                // 2)位数不足补0，ipv4地址中一组可转换成一个十六进制，两组数即可标识ipv6中的一组，v6中的一组数不足4位补0
                for (int k = 0; k < (IPV4ParmLength - arrParams[pFlag].length()); k++) {
                    Key += "0";
                }
                Key += arrParams[pFlag];
                pFlag++;
            }
        }
        // ipv6 address。将ipv6地址转换成16进制
        if (dColonFlag == -1 && dotFlag == -1 && colonFlag != -1) {
            String[] arrParams = ipAddress.split("&");
            // 将v6地址转成十六进制
            for (int i = 0; i < IPV6Length; i++) {
                // 将ipv6地址中每组不足4位的补0
                for (int k = 0; k < (IPV6ParmLength - arrParams[i].length()); k++) {
                    Key += "0";
                }

                Key += arrParams[i];
            }
        }

        if (dColonFlag != -1 && dotFlag == -1) {
            String[] arr = ipAddress.split("&");
            String[] arrParams = new String[IPV6Length];
            int indexFlag = 0;
            if (arr.length == 0) {
                for (int j = 0; j < IPV6Length; j++) {
                    Key = Key + "0000";
                }
                return Key;
            }
            if ("".equals(arr[0])) {
                for (int j = 0; j < (IPV6Length - (arr.length - 2)); j++) {
                    arrParams[j] = "0000";
                    indexFlag++;
                }
                for (int i = 2; i < arr.length; i++) {
                    arrParams[indexFlag] = arr[i];
                    i++;
                    indexFlag++;
                }
            } else {
                for (int i = 0; i < arr.length; i++) {
                    if ("".equals(arr[i])) {
                        for (int j = 0; j < (IPV6Length - arr.length + 1); j++) {
                            arrParams[indexFlag] = "0000";
                            indexFlag++;
                        }
                    } else {
                        arrParams[indexFlag] = arr[i];
                        indexFlag++;
                    }
                }
            }
            for (int i = 0; i < IPV6Length; i++) {
                if (org.apache.commons.lang.StringUtils.isEmpty(arrParams[i])) {
                    arrParams[i] = "0000";
                }
                for (int k = 0; k < (IPV6ParmLength - arrParams[i].length()); k++) {
                    Key += "0";
                }
                Key += arrParams[i];
            }
        }

        return Key;
    }

    /**
     * 十六进制串转化为IP地址
     *
     * @param key
     * @return
     */
    private static String splitKey(String key) {
        String IPV6Address = "";
        String IPAddress = "";
        String strKey = "";
        String ip1 = key.substring(0, 24);
        String tIP1 = ip1.replace("0000", "").trim();
        if (!"".equals(tIP1) && !"FFFF".equals(tIP1)) {
            // 将ip按：分隔
            while (!"".equals(key)) {
                strKey = key.substring(0, 4);
                key = key.substring(4);
                if ("".equals(IPV6Address)) {
                    IPV6Address = strKey;
                } else {
                    IPV6Address += ":" + strKey;
                }
            }
            IPAddress = IPV6Address;
        }
        return IPAddress;
    }
}
