package com.lovecws.mumu.flink.common.util;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * @program: jmr
 * @description: ip归属地查询
 * @author: 甘亮
 * @create: 2018-12-03 10:19
 **/
public class IPhoneAddressUtil {
    private static final String IP_ADDRESS_PATH = "data/H20180425.csv";
    private static final Logger log = Logger.getLogger(IPhoneAddressUtil.class);
    private static final String IP_ADDRESS_SEPEATOR = ",";
    private static final Map<String, String> IP_ADDRESS_DATA = new HashMap<>();

    static {
        load();
    }

    /**
     * 加载ipAddress
     */
    public static synchronized void load() {
        if (IP_ADDRESS_DATA.size() > 0) {
            return;
        }
        BufferedReader bufferedReader = null;
        try {
            bufferedReader = new BufferedReader(new InputStreamReader(ClassLoader.getSystemResourceAsStream(IP_ADDRESS_PATH)));
            String readLine = null;
            boolean firstLine = true;
            while ((readLine = bufferedReader.readLine()) != null) {
                if (firstLine) {
                    firstLine = false;
                    continue;
                }
                String[] fields = readLine.split(IP_ADDRESS_SEPEATOR);
                IP_ADDRESS_DATA.put(fields[0], readLine);
            }
        } catch (Exception e) {
            log.error(e.getLocalizedMessage(), e);
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    log.error(e.getLocalizedMessage(), e);
                }
            }
        }
    }

    /**
     * 根据手机号码获取手机号码的归属地
     *
     * @param phone 手机号码
     * @return 手机号码归属地map
     */
    public static Map<String, String> address(String phone) {
        Map<String, String> map = new HashMap<>();
        if (phone == null || phone.length() < 7) {
            return map;
        }
        if (phone.startsWith("86")) {
            phone = phone.substring(2);
        }
        String line = IP_ADDRESS_DATA.get(phone.substring(0, 7));
        if (line != null) {
            String[] fields = line.split(IP_ADDRESS_SEPEATOR);
            String operator = fields[1];
            if (operator.contains("移动")) {
                operator = "移动";
            } else if (operator.contains("电信")) {
                operator = "电信";
            } else if (operator.contains("联通")) {
                operator = "联通";
            }
            map.put("operator", operator);
            map.put("company", fields[2]);
            map.put("province", fields[3]);
            map.put("city", fields[4]);
        }
        return map;
    }
}
