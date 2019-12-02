package com.lovecws.mumu.flink.common.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StorageFilterUtil {

	protected static Logger log = LoggerFactory.getLogger(StorageFilterUtil.class);

	/**
	 * 判断object在ruleString下，是否应该过滤
	 * 
	 * @param object 判断的对象
	 * @param ruleString 过滤规则
	 * @return
	 */
	public static boolean filter(Object object, String ruleString) {
		return work(object, ruleString, null);
	}

	public static boolean filter(Object object, String ruleString, Class cls) {
		return work(object, ruleString, cls);
	}

	private static boolean work(Object object, String ruleString, Class cls) {
		boolean filter = false;

		if (StringUtils.isBlank(ruleString)) {
		    return filter;
        }

		List<String> ruleList = new ArrayList<>();
		List<String> keyList = new ArrayList<>();
		// 获取解析规则
		parseRule(ruleString, ruleList, keyList);

		// 是否进行聚合操作
		String combine = null;
		for(int i = 0; i < ruleList.size(); i++){
			boolean tmp = false;

			String rule = ruleList.get(i);
			String ruleKey = keyList.get(i);

			// 如果为and、or则准备进行聚合操作
			if (rule.equals("and") || rule.equals("or")) {
				combine = rule;
				continue;
			}

			if (rule.equals("!")) {
				tmp = StringUtils.isBlank((String)getValue(object, ruleKey, cls));
			}

			if (StringUtils.isNotBlank(combine)) {
				if (combine.equals("and")) {
					filter = filter & tmp;
				} else if (combine.equals("or")) {
					filter = filter | tmp;
				}
				combine = null;
			} else {
				filter = tmp;
			}
		}
		return filter;
	}

	private static Object getValue(Object object, String key, Class cls) {
		if (cls == null) {
			return ((Map) object).get(key);
		}
		// TODO:如果不是map，可以通过反射获取value

		return null;
	}

	private static void parseRule(String ruleString, List<String> ruleList, List<String> keyList) {
		// 使用and做切分，目前只做简单处理
		String[] andSplit = ruleString.split(" ");
		for (String tmp : andSplit) {
			if (tmp.startsWith("!")) {
				ruleList.add("!");
				keyList.add(tmp.substring(1));
			} else if (tmp.equals("and") || tmp.equals("or")) {
				ruleList.add(tmp);
				keyList.add("");
			}
		}
	}

}
