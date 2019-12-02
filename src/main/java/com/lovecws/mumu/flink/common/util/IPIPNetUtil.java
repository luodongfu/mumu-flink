package com.lovecws.mumu.flink.common.util;

import com.alibaba.fastjson.JSON;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class IPIPNetUtil implements Serializable {

    public static boolean enableFileWatch = false;

    private static int offset;
    private static int[] index = new int[65536];
    private static ByteBuffer dataBuffer;
    private static ByteBuffer indexBuffer;
    private static ReentrantLock lock = new ReentrantLock();
    private static final String IP_DATA_PATH = "data/IPIP.datx";
    private static final Logger log = Logger.getLogger(IPIPNetUtil.class);

    public static void load(String filePath) {
        loadData(filePath);
    }

    public static Map<String, String> get(String ip) {
        Map<String, String> resultMap = new HashMap<>();
        if (StringUtils.isEmpty(ip)) {
            return resultMap;
        }
        //过滤ipv6的ip地址信息
        if ("ipv6".equalsIgnoreCase(IPUtil.ipType(ip))) {
            return resultMap;
        }

        try {
            String[] ipipnet = find(ip);
            resultMap.put("contry", ipipnet[0]);
            resultMap.put("contryCode", ipipnet[10]);
            resultMap.put("province", ipipnet[1]);

            resultMap.put("city", ipipnet[2]);
            resultMap.put("cityCode", ipipnet[9]);
            resultMap.put("operator", ipipnet[4]);
            resultMap.put("longitude", ipipnet[5]);
            resultMap.put("latitude", ipipnet[6]);
            String provinceCode = ipipnet[9];
            if (provinceCode != null && provinceCode.length() > 2) {
                provinceCode = provinceCode.substring(0, 2) + "0000";
            }
            resultMap.put("provinceCode", provinceCode);

            resultMap.put("idc", ipipnet.length > 12 ? ipipnet[12] : "");

            //ip所属国家
            resultMap.put("country", ipipnet.length > 0 ? ipipnet[0] : "");
            //ip所属省份
            resultMap.put("province", ipipnet.length > 1 ? ipipnet[1] : "");
            //ip地级市/省直辖县级行政区
            resultMap.put("city", ipipnet.length > 2 ? ipipnet[2] : "");
            //ip段所有者
            resultMap.put("owner", ipipnet.length > 3 ? ipipnet[3] : "");
            //ip运营商
            resultMap.put("ipOperator", ipipnet.length > 4 ? ipipnet[4] : "");
            //ip维度
            resultMap.put("ipLongitude", ipipnet.length > 5 ? ipipnet[5] : "");
            //ip经度
            resultMap.put("ipLatitude", ipipnet.length > 6 ? ipipnet[6] : "");
            //时区代表城市
            resultMap.put("timeZoneCity", ipipnet.length > 7 ? ipipnet[7] : "");
            //时区
            resultMap.put("timeZone", ipipnet.length > 8 ? ipipnet[8] : "");
            //中国行政区划代码
            resultMap.put("areaCode", ipipnet.length > 9 ? ipipnet[9] : "");
            //国际区号
            resultMap.put("globalRoaming", ipipnet.length > 10 ? ipipnet[10] : "");
            //国际代码
            resultMap.put("internalCode", ipipnet.length > 11 ? ipipnet[11] : "");
            //州代码
            resultMap.put("stateCode", ipipnet.length > 12 ? ipipnet[12] : "");
            //ip所属区域
            resultMap.put("ipArea", ipipnet.length > 9 ? ipipnet[9] : "");
        } catch (Exception ex) {
            log.error(ex.getLocalizedMessage() + "[" + ip + "]", ex);
        }
        return resultMap;
    }

    public static String[] find(String ip) {
        if (dataBuffer == null || indexBuffer == null) {
            synchronized (IPIPNetUtil.class) {
                if (dataBuffer == null || indexBuffer == null) {
                    load(IP_DATA_PATH);
                }
            }
        }
        String[] ips = ip.split("\\.");
        int prefix_value = (Integer.valueOf(ips[0]) * 256 + Integer.valueOf(ips[1]));
        long ip2long_value = ip2long(ip);
        int start = index[prefix_value];
        int max_comp_len = offset - 262144 - 4;
        long tmpInt;
        long index_offset = -1;
        int index_length = -1;
        byte b = 0;
        for (start = start * 9 + 262144; start < max_comp_len; start += 9) {
            tmpInt = int2long(indexBuffer.getInt(start));
            if (tmpInt >= ip2long_value) {
                index_offset = bytesToLong(b, indexBuffer.get(start + 6), indexBuffer.get(start + 5), indexBuffer.get(start + 4));
                index_length = ((0xFF & indexBuffer.get(start + 7)) << 8) + (0xFF & indexBuffer.get(start + 8));
                break;
            }
        }

        return findByByte(index_offset, index_length);
    }

    /**
     * 通过加锁的方式获取到databuffer数据
     *
     * @param index_offset 偏移量
     * @param index_length 截取字节数量
     * @return
     */
    private static String[] findByLock(long index_offset, int index_length) {
        byte[] areaBytes;
        lock.lock();
        try {
            dataBuffer.position(offset + (int) index_offset - 262144);
            areaBytes = new byte[index_length];
            dataBuffer.get(areaBytes, 0, index_length);
        } finally {
            lock.unlock();
        }
        return new String(areaBytes, Charset.forName("UTF-8")).split("\t", -1);
    }

    /**
     * 通过将databuffer转化为字节数组来截取字节
     *
     * @param index_offset 偏移量
     * @param index_length 截取字节数量
     * @return
     */
    private static String[] findByByte(long index_offset, int index_length) {
        byte[] byteArray = dataBuffer.array();
        int beginIndex = offset + (int) index_offset - 262144;
        byte[] bytes = Arrays.copyOfRange(byteArray, beginIndex, beginIndex + index_length);
        return new String(bytes, Charset.forName("UTF-8")).split("\t", -1);
    }


    private static void loadData(String filePath) {
        lock.lock();
        try {
            dataBuffer = ByteBuffer.wrap(getBytesByFile(filePath));
            dataBuffer.position(0);
            offset = dataBuffer.getInt(); // indexLength
            byte[] indexBytes = new byte[offset];
            dataBuffer.get(indexBytes, 0, offset - 4);
            indexBuffer = ByteBuffer.wrap(indexBytes);
            indexBuffer.order(ByteOrder.LITTLE_ENDIAN);

            for (int i = 0; i < 256; i++) {
                for (int j = 0; j < 256; j++) {
                    index[i * 256 + j] = indexBuffer.getInt();
                }
            }
            indexBuffer.order(ByteOrder.BIG_ENDIAN);
        } finally {
            lock.unlock();
        }
    }

    public static ClassLoader getDefaultClassLoader() {
        ClassLoader cl = null;
        try {
            cl = Thread.currentThread().getContextClassLoader();
        } catch (Throwable ex) {
            // Cannot access thread context ClassLoader - falling back...
        }
        if (cl == null) {
            // No thread context class loader -> use class loader of this class.
            cl = IPIPNetUtil.class.getClassLoader();
            if (cl == null) {
                // getClassLoader() returning null indicates the bootstrap ClassLoader
                try {
                    cl = ClassLoader.getSystemClassLoader();
                } catch (Throwable ex) {
                    // Cannot access system ClassLoader - oh well, maybe the caller can live with null...
                }
            }
        }
        return cl;
    }

    private static byte[] getBytesByFile(String filePath) {
        InputStream fin = null;
        byte[] bs = null;
        try {
            fin = ClassLoader.getSystemResourceAsStream(filePath);
            if (fin == null) {
                fin = getDefaultClassLoader().getResourceAsStream(filePath);
            }
            bs = new byte[fin.available()];
            IOUtils.read(fin, bs);

            /*int readBytesLength = 0;
            int i;
            while ((i = fin.available()) > 0) {
                fin.read(bs, readBytesLength, i);
                readBytesLength += i;
            }*/
        } catch (IOException ioe) {
            log.error(ioe.getLocalizedMessage(), ioe);
        } finally {
            try {
                if (fin != null) {
                    fin.close();
                }
            } catch (IOException e) {
                log.error(e.getLocalizedMessage(), e);
            }
        }

        return bs;
    }

    private static long bytesToLong(byte a, byte b, byte c, byte d) {
        return int2long((((a & 0xff) << 24) | ((b & 0xff) << 16) | ((c & 0xff) << 8) | (d & 0xff)));
    }

    private static int str2Ip(String ip) {
        String[] ss = ip.split("\\.");
        int a, b, c, d;
        a = Integer.parseInt(ss[0]);
        b = Integer.parseInt(ss[1]);
        c = Integer.parseInt(ss[2]);
        d = Integer.parseInt(ss[3]);
        return (a << 24) | (b << 16) | (c << 8) | d;
    }

    private static long ip2long(String ip) {
        return int2long(str2Ip(ip));
    }

    private static long int2long(int i) {
        long l = i & 0x7fffffffL;
        if (i < 0) {
            l |= 0x080000000L;
        }
        return l;
    }

    public static void main(String[] args) {
        Map<String, String> stringStringMap = get("59.173.199.235");
        System.out.println(JSON.toJSONString(stringStringMap));
    }
}