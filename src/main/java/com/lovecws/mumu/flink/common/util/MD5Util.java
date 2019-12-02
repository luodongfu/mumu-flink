package com.lovecws.mumu.flink.common.util;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * MD5实用类
 * 
 * @author Administrator
 * 
 */
public class MD5Util {

	protected static MessageDigest messagedigest = null;
	static {
		try {
			messagedigest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException nsaex) {
			System.err.println(MD5Util.class.getName()
					+ "初始化失败，MessageDigest不支持MD5Util。");
			nsaex.printStackTrace();
		}
	}

	/**
	 * 生成字符串的md5校验值
	 * 
	 * @param str
	 * @return
	 * @throws UnsupportedEncodingException 
	 */
	public static byte[] getMD5Encode(String str) throws UnsupportedEncodingException {
		return getMD5Encode(str.getBytes("UTF-8"));
	}

	/**
	 * 生成字符串的md5校验值
	 * 
	 * @param bytes
	 * @return
	 * @throws UnsupportedEncodingException 
	 */
	public static byte[] getMD5Encode(byte[] bytes) throws UnsupportedEncodingException {
		messagedigest.update(bytes);
		bytes = messagedigest.digest();
		
		StringBuilder ret = new StringBuilder(bytes.length << 1);
		for (int i = 0; i < bytes.length; i++) {
			ret.append(Character.forDigit((bytes[i] >> 4) & 0xf, 16));
			ret.append(Character.forDigit(bytes[i] & 0xf, 16));
		}
		String str = ret.toString();
		return str.getBytes("UTF-8");
	}

	/**
	 * 对文本执行 md5 摘要加密, 此算法与 mysql,JavaScript生成的md5摘要进行过一致性对比.
	 *
	 * @param plainText
	 * @return 返回值中的字母为小写
	 */
	public static String md5(String plainText) {
		if (null == plainText) {
			plainText = "";
		}
		String MD5Str = "";
		try {
			// JDK 6 支持以下6种消息摘要算法，不区分大小写
			// md5,sha(sha-1),md2,sha-256,sha-384,sha-512
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(plainText.getBytes());
			byte b[] = md.digest();

			int i;

			StringBuilder builder = new StringBuilder(32);
			for (int offset = 0; offset < b.length; offset++) {
				i = b[offset];
				if (i < 0)
					i += 256;
				if (i < 16)
					builder.append("0");
				builder.append(Integer.toHexString(i));
			}
			MD5Str = builder.toString();
			// LogUtil.println("result: " + buf.toString());// 32位的加密
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return MD5Str;
	}
}