/**
 * Project Name:ism
 * File Name:ISMIUtil.java
 * Date:2013年11月28日上午10:08:03
 *
*/

package com.lovecws.mumu.flink.common.util;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;


/**
 * ClassName:ISMIUtil <br/>
 * Date:     2013年11月28日 上午10:08:03 <br/>
 * @author   huanglian
 * @version  
 * @since    JDK 1.6
 * @see 	 
 */
public class EncryDecryUtil {
	
	protected static Logger logger = LogManager.getLogger(EncryDecryUtil.class);
	
//	protected static MessageDigest messagedigest = null;
//	
//	static {
//		try {
//			messagedigest = MessageDigest.getInstance("MD5");
//		} catch (NoSuchAlgorithmException nsaex) {
//			System.err.println("初始化失败，MessageDigest不支持MD5Util。");
//			nsaex.printStackTrace();
//		}
//	}
	
	/**
	 * 密码Hash
	 * 
	 * 将用户口令和随机字符串连接后使用hashAlgorithm指定的哈希算法进行哈希运算，然后进行base64编码得到的结果。
	 * 用户口令由设备所在SMMS维护管理，长度至少为6位，最多32位
	 * @param userKey 密码
	 * @param randVal 随机数
	 * @param hashAlgorithm 哈希算法
	 * @return
	 */
	public static String pwdHash(String userKey, String randVal,int hashAlgorithm) {
		// 串接消息认证密钥
		String pwdTmp = userKey + randVal;
		try {
			byte[] result = pwdTmp.getBytes("UTF-8");
			// 使用hashAlgorithm指定的哈希算法进行哈希运算
			if (hashAlgorithm==1) {
				result = getMD5Encode(pwdTmp);
			} else if (hashAlgorithm==2) {
				result = getSHA1Encode(pwdTmp);
			} else {
				logger.info("pwdHash方式为不hash");
			}
			result = getBASE64(result);
			return new String(result, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			logger.error("不支持此字符集", e);
		}
		return null;
	}

	/**
	 * 指令Hash
	 * @param commandHash
	 * @param authenticationKey
	 * @param zipDir
	 * @param fileName
	 * @param hashAlgorithm
	 * @param compressionFormat
	 * @return
	 */
	public static String commandHash(String commandHash, String authenticationKey, String zipDir, String fileName,int hashAlgorithm,int compressionFormat) {
		// 使用compressionFormat指定的压缩算法进行压缩
		byte[] commandHashBytes = null;
		try {
			commandHashBytes = commandHash.getBytes("UTF-8");
			commandHashBytes = commandZip(zipDir, fileName, commandHash,compressionFormat);
			// 串接消息认证密钥
			commandHashBytes = joinBytes(commandHashBytes, authenticationKey.getBytes("UTF-8"));
			// 使用hashAlgorithm指定的哈希算法进行哈希运算
			long start = System.currentTimeMillis();
			if (hashAlgorithm==1) {
				commandHashBytes = getMD5Encode(commandHashBytes);
				logger.info("MD5_Hash花费时间：" + ((System.currentTimeMillis() - start)/1000) + "s");
			} else if (hashAlgorithm==2) {
				// 串接消息认证密钥
				commandHashBytes = getSHA1Encode(commandHashBytes);
				logger.info("SHA1_Hash花费时间：" + ((System.currentTimeMillis() - start)/1000) + "s");
			} else {
				logger.info("不Hash");
			}
			long start2 = System.currentTimeMillis();
			commandHashBytes = getBASE64(commandHashBytes);
			logger.info("Base64花费时间：" + ((System.currentTimeMillis() - start2)/1000) + "s");
			return new String(commandHashBytes, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			logger.error("commandHash 不支持此字符集", e);
		}
		return null;
	}
	
	/**
	 * 指令加密
	 * @param command
	 * @param encryptionKey
	 * @param asepyl
	 * @param zipDir
	 * @param fileName
	 * @param encryptAlgorithm
	 * @param compressionFormat
	 * @return
	 */
	public static String commandEncrypt(String command, String encryptionKey, String asepyl, String zipDir, String fileName,int encryptAlgorithm,int compressionFormat) {
		// 使用compressionFormat指定的压缩算法进行压缩
		try {
			byte[] commandBytes = command.getBytes("UTF-8");
			commandBytes = commandZip(zipDir, fileName, command,compressionFormat);
			// 按照encryptAlgorithm参数的要求进行加密
			if (encryptAlgorithm==1) {
				try {
					long start = System.currentTimeMillis();
					commandBytes = encrypt(commandBytes, encryptionKey, asepyl);
					logger.info("AES加密花费时间：" + ((System.currentTimeMillis() - start)/1000) + "s");
				} catch (Exception e) {
					logger.error("指令下发加密出错：", e);
					return null;
				}
			} else {
				logger.info("不AES加密和Base64");
			}
			// 进行Base64编码运算
			long start2 = System.currentTimeMillis();
			commandBytes = getBASE64(commandBytes);
			logger.info("Base64花费时间：" + ((System.currentTimeMillis() - start2)/1000) + "s");
			
			return new String(commandBytes, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			logger.error("commandEncrypt 不支持此字符集", e);
		}
		return null;
	}
	
	/**
	 * 压缩方法，将其单独提出来，避免command和commandHash两个参数做两次压缩影响效率
	 * @param zipDir 压缩目录
	 * @param fileName 压缩文件名
	 * @param command XML
	 * @return 压缩后的XML值
	 */
	public static byte[] commandZip(String zipDir, String fileName, String command,int compressionFormat) {
		// 使用compressionFormat指定的压缩算法进行压缩
		byte[] contentBytes = null;
		try {
			contentBytes = command.getBytes("UTF-8");
			if (compressionFormat==1) {
				long start = System.currentTimeMillis();
				File zipFile = zip(zipDir, fileName, command);
				try {
					contentBytes= FileUtils.readFileToByteArray(zipFile);
				} catch (IOException e) {
					logger.error("写入zip ack文件时出现异常", e);
				}
				logger.info("Zip花费时间：" + ((System.currentTimeMillis() - start)/1000) + "s");
			} else {
				logger.info("不压缩");
			}
		} catch (UnsupportedEncodingException e) {
			logger.info("不支持此字符集", e);
		}
		return contentBytes;
	}
	
	/**
	 * 根据文件内容生成zip文件
	 * @param path
	 * @param content
	 * @return
	 */
	public static File zip(String path, String fileName, String content) {
		if (content == null) {
			return null;
		}
		
		File zipDir = new File(path);
		if(!zipDir.exists()){
			zipDir.mkdirs();
        }
		String zipName = path + File.separator + fileName + ".zip";
		
		File zipFile = new File(zipName);
		
		FileOutputStream out = null;
		ZipOutputStream zout = null;
		try {
			out = new FileOutputStream(zipFile);
			zout = new ZipOutputStream(out);
			zout.putNextEntry(new ZipEntry(fileName + ".xml"));
			zout.write(content.getBytes("UTF-8"));
		} catch (IOException e) {
			e.getStackTrace();
			logger.error("压缩文件报错：", e);
		} finally {
			if (zout != null) {
				try {
					zout.flush();
					zout.close();
				} catch (IOException e) {
					e.getStackTrace();
					logger.error("压缩文件报错：", e);
				}
			}
			if (out != null) {
				try {
					out.flush();
					out.close();
				} catch (IOException e) {
					e.getStackTrace();
					logger.error("压缩文件报错：", e);
				}
			}
		}
		return zipFile;
	}
	
	// 将 s 进行 BASE64 编码
	public static byte[] getBASE64(byte[] contentBytes) {
		if (contentBytes == null)
			return null;
		byte[] base64EncodeBytes = null;
		base64EncodeBytes = org.apache.commons.codec.binary.Base64.encodeBase64(contentBytes);
		return base64EncodeBytes;
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
		MessageDigest messagedigest = null;
		try {
			messagedigest = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException nsaex) {
			System.err.println("初始化失败，MessageDigest不支持MD5Util。");
			nsaex.printStackTrace();
		}
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
	 * 生成随机数,长度是20字节
	 * 
	 * @return
	 */
	public static String genRandomString() {
		Random rand = new Random(System.nanoTime());
		StringBuffer sb = new StringBuffer(20);
		char head[] = { '0', 'A', 'a' };
		while (sb.length() != 20) {
			int num = rand.nextInt(78);
			int x = num / 26;
			int y = num % 26;
			if (x == 0 && y >= 10)
				continue;
			sb.append((char) (head[x] + y));
		}
		return sb.toString();
	}
	
	/**
	 * 
	 * @param bytes
	 * @return
	 * @throws UnsupportedEncodingException
	 * @since JDK 1.6
	 */
	public static byte[] getSHA1Encode(byte[] bytes) throws UnsupportedEncodingException {
		MessageDigest messagedigest = null;
		try {
			messagedigest = MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException nsaex) {
			System.err.println("初始化失败，MessageDigest不支持MD5Util。");
			nsaex.printStackTrace();
		}
		bytes = messagedigest.digest(bytes);
		StringBuilder ret = new StringBuilder(bytes.length << 1);
		for (int i = 0; i < bytes.length; i++) {
			ret.append(Character.forDigit((bytes[i] >> 4) & 0xf, 16));
			ret.append(Character.forDigit(bytes[i] & 0xf, 16));
		}
		String str = ret.toString();
		return str.getBytes("UTF-8");
	}

	/**
	 * 
	 * @param str
	 * @return
	 * @throws UnsupportedEncodingException
	 * @since JDK 1.6
	 */
	public static byte[] getSHA1Encode(String str)
			throws UnsupportedEncodingException {
		return getSHA1Encode(str.getBytes("UTF-8"));
	}
	
	/**
	 * 加密
	 * @param sSrc
	 * @param sKey 加密密钥
	 * @return
	 * @throws Exception
	 */
	private static byte[] encrypt(byte[] sSrc, String sKey) throws Exception {
		if (sKey == null) {
			return null;
		}
		byte[] raw = sKey.getBytes("UTF-8");
		SecretKeySpec skeySpec = new SecretKeySpec(raw, 0, 16, "AES");
		Cipher cipher = Cipher.getInstance("AES");
		cipher.init(Cipher.ENCRYPT_MODE, skeySpec);
		byte[] encrypted = cipher.doFinal(sSrc);
		return encrypted;
	}
	
	/**
	 * 加密 - 带偏移量
	 * @param sSrc
	 * @param encryptionKey 加密密钥
	 * @param asepyl 偏移量
	 * @return
	 * @throws Exception
	 */
	public static byte[] encrypt(byte[] sSrc, String encryptionKey, String asepyl) throws Exception {
		if (encryptionKey == null) {
			return null;
		}
		if (StringUtils.isBlank(asepyl)) {
			return encrypt(sSrc, encryptionKey);
		}
		byte[] raw = encryptionKey.getBytes("UTF-8");
		SecretKeySpec keySpec = new SecretKeySpec(raw, "AES");
		Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
		IvParameterSpec initVector = new IvParameterSpec(asepyl.getBytes(), 0 , 16);
		cipher.init(Cipher.ENCRYPT_MODE, keySpec, initVector);
		byte[] encrypted = cipher.doFinal(sSrc);
		return encrypted;
	}
	

	/**
	 * 将srcByte2连接到srcByte1后组成一个byte数组
	 * 
	 * @param srcByte1 数组1
	 * @param srcByte2 数组2
	 * @return 串接srcByte1和srcByte2的数组
	 */
	public static final byte[] joinBytes(byte[] srcByte1, byte[] srcByte2) {
		int srcByte1Length = srcByte1.length;
		int srcByte2Length = srcByte2.length;
		byte[] destByte = new byte[srcByte1Length + srcByte2Length];
		System.arraycopy(srcByte1, 0, destByte, 0, srcByte1Length); //copy第一个数组
		System.arraycopy(srcByte2, 0, destByte, srcByte1Length, srcByte2Length); //copy第二个数组
		return destByte;
	}

	// 将 BASE64 编码的字符串 s 进行解码
	public static byte[] getFromBASE64(String s) {
		if (s == null)
			return null;
		byte[] b = null;
		try {
			b = org.apache.commons.codec.binary.Base64.decodeBase64(s.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			logger.error("decodeBase64 出现异常", e);
		}
		return b;
	}
	
	public static byte[] unzip(byte[] content) {
		if (content == null) {
			return null;
		}
		ByteArrayOutputStream out = null;
		ByteArrayInputStream in = null;
		ZipInputStream zin = null;
		byte[] unzipResult = null;
		try {
			out = new ByteArrayOutputStream();
			in = new ByteArrayInputStream(content);
			zin = new ZipInputStream(in);
			zin.getNextEntry();
			byte[] buffer = new byte[1024];
			int offset = -1;
			while ((offset = zin.read(buffer)) != -1) {
				out.write(buffer, 0, offset);
			}
			unzipResult = out.toByteArray();
		} catch (IOException e) {
			logger.error("解压文件报错：", e);
		} finally {
			if (zin != null) {
				try {
					zin.close();
				} catch (IOException e) {
					logger.error("文件流报错：", e);
				}
			}
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					logger.error("解压文件报错：", e);
				}
			}
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					logger.error("解压文件报错：", e);
				}
			}
		}

		return unzipResult;
	}
	
	/**
	 * 解密 - 带偏移量
	 * @param sSrc
	 * @param sKey
	 * @param spec
	 * @return
	 * @throws Exception
	 */
	public static byte[] decrypt(byte[] sSrc, String sKey, String spec) throws Exception {
		if (sKey == null) {
			return null;
		}
		if (StringUtils.isBlank(spec)) {
			return decrypt(sSrc, sKey);
		}
		byte[] raw = sKey.getBytes("UTF-8");
		SecretKeySpec skeySpec = new SecretKeySpec(raw, 0, 16, "AES");
		Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
		IvParameterSpec initVector = new IvParameterSpec(spec.getBytes(), 0 , 16);
		cipher.init(Cipher.DECRYPT_MODE, skeySpec, initVector);
		byte[] original = cipher.doFinal(sSrc);
		return original;
	}
	
	/**
	 * 解密
	 * @param sSrc
	 * @param sKey
	 * @return
	 * @throws Exception
	 */
	private static byte[] decrypt(byte[] sSrc, String sKey) throws Exception {
		// 判断Key是否正确
		if (sKey == null) {
			return null;
		}

		byte[] raw = sKey.getBytes("UTF-8");
		SecretKeySpec skeySpec = new SecretKeySpec(raw, 0, 16, "AES");
		Cipher cipher = Cipher.getInstance("AES");
		cipher.init(Cipher.DECRYPT_MODE, skeySpec);
		byte[] original = cipher.doFinal(sSrc);
		return original;
	}
	
	 /**
     * 获取count个随机数
     * @param count 随机数个数
     * @return
     */
    public static  String getRandomNumStr(int count){
        StringBuffer sb = new StringBuffer();
        String str = "0123456789";
        Random r = new Random();
        for(int i=0;i<count;i++){
            int num = r.nextInt(str.length());
            sb.append(str.charAt(num));
            str = str.replace((str.charAt(num)+""), "");
        }
        return sb.toString();
    }
 
    public static byte[] compression(int compressType,String xml) throws IOException {
		byte[] result=null;
		if(StringUtils.isNotBlank(xml)){
			if(compressType==1){
				result=xml.getBytes("utf-8");
				ByteArrayOutputStream baos=null;
				ZipOutputStream zout=null;
				try {
					baos=new ByteArrayOutputStream();
					zout=new ZipOutputStream(baos);
					zout.putNextEntry(new ZipEntry(System.currentTimeMillis()+".xml"));
					zout.write(result);
					zout.closeEntry();
					result=baos.toByteArray();
				} finally{
					if(zout!=null){
						zout.close();
					}
					if(baos!=null){
						baos.close();
					}
				}
			}else{
				result=xml.getBytes("utf-8");
			}
		}
		return result;
	}
    
}

