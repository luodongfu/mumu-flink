package com.lovecws.mumu.flink.common.util;

import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;

/**
 * @program: act-able
 * @description: 过滤特殊字符
 * @author: 甘亮
 * @create: 2019-06-20 11:32
 **/
public class EscapeUtil {

    private static final Escaper CLICKHOUSE_ESCAPER = Escapers.builder()
            .addEscape('\\', "\\\\")
            .addEscape('\n', "\\n")
            .addEscape('\t', "\\t")
            .addEscape('\b', "\\b")
            .addEscape('\f', "\\f")
            .addEscape('\r', "\\r")
            .addEscape('\u0000', "\\0")
            .addEscape('\'', "\\'")
            .addEscape('`', "\\`")
            .build();

    public EscapeUtil() {
    }

    public static String escape(String s) {
        return s == null ? "\\N" : CLICKHOUSE_ESCAPER.escape(s);
    }

    public static String quoteIdentifier(String s) {
        if (s == null) {
            throw new IllegalArgumentException("Can't quote null as identifier");
        } else {
            StringBuilder sb = new StringBuilder(s.length() + 2);
            sb.append('`');
            sb.append(CLICKHOUSE_ESCAPER.escape(s));
            sb.append('`');
            return sb.toString();
        }
    }
}
