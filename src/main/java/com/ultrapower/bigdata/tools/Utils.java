package com.ultrapower.bigdata.tools;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public class Utils {

	public static final String BEGIN="{$" ;
	public static final String END="}" ;
	
	/**
	 * 替换查询参数
	 * @param sql
	 * @param map
	 */
	public static String parse(String sql, Map<String, String> map)
	{
		int begin = sql.indexOf(BEGIN) ;
		while(begin != -1)
		{
			String suffix = sql.substring(begin + BEGIN.length());
			int end = begin + BEGIN.length() + suffix.indexOf(END) ;
			String key = sql.substring(begin+BEGIN.length(), end).trim() ;
			if (map != null && map.get(key) != null) {
				sql = sql.substring(0, begin) + map.get(key) + sql.substring(end + 1, sql.length()) ;
			}
			else
			{
				throw new RuntimeException("Invalid Expression.....");
			}
			begin = sql.indexOf(BEGIN) ;
		}
		return sql ;
	}
	
	public static Date StrToDate(String str) {
		//SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		Date date = null;
		try {
			date = format.parse(str);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date;
	}
}
