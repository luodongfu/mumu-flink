/**
 * Project Name:application-business
 * File Name:IdCreator.java
 * Package Name:com.surfilter.dhp.is2.commons.util
 * Date:2014年4月19日下午4:23:33
 *
*/

package com.lovecws.mumu.flink.common.util;

import java.util.Random;

/**
 * ClassName:IdCreator <br/>
 * Date:     2014年4月19日 下午4:23:33 <br/>
 * @author   dengqw
 * @version  
 * @since    JDK 1.6
 * @see 	 
 */
public class IdCreator {
	public static long getId(){
		Random random =new Random();
		int offset=random.nextInt(999999);
		return System.currentTimeMillis()*1000000+offset;
	}
}

