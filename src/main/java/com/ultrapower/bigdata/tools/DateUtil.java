package com.ultrapower.bigdata.tools;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;



public class DateUtil {
	public static final Logger log = Logger.getLogger(DateUtil.class);
	private static Map<String, Date> dateMap = new HashMap<String, Date>();
	/**
	 * 获取月份时间字符串
	 * @return
	 */
	private static String getMonth(int i){
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd"); 
		GregorianCalendar gcd = (GregorianCalendar) Calendar.getInstance(); 
		gcd.add(Calendar.MONTH, i);
		gcd.set(Calendar.DAY_OF_MONTH, 1);
		String date = df.format(gcd.getTime());
		return date;
	}
	
	/**
	 * 根据时间字符串获得天
	 * @param dateStr
	 * @return
	 */
	public static int getDay(String dateStr){
		GregorianCalendar gcd = (GregorianCalendar) Calendar.getInstance();
		String format = dateStr.indexOf("-") != -1 ? "yyyy-MM-dd" : "yyyy/MM/dd";
		Date dt = StringToDate(dateStr,format);
		gcd.setTime(dt);
		int day = gcd.get(Calendar.DATE);
		return day;
	}
	/**
	 * 验证非法时间字符串
	 * @param date
	 * @return
	 */
	private static boolean isDate(String date){
		boolean flag = true;
		if(date == null || "".equals(date)){
			flag = false;
		}else{
			String eL= "^((\\d{2}(([02468][048])|([13579][26]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])))))|(\\d{2}(([02468][1235679])|([13579][01345789]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|(1[0-9])|(2[0-8]))))))(\\s(((0?[0-9])|([1][0-9])|([2][0-3]))\\:([0-5]?[0-9])((\\s)|(\\:([0-5]?[0-9])))))?$";
            Pattern p = Pattern.compile(eL);         
            Matcher m = p.matcher(date);         
            flag = m.matches();    
		}
		return flag;
	}
	
	/**
	 * 获取当前月的第一天或下个月的第一天时间字符串
	 * @param date
	 * @param i
	 * @return
	 */
	public static String getMonthFirstDay(String date, int i){
		StringBuffer dateStr = new StringBuffer();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd"); 
		GregorianCalendar gc = (GregorianCalendar) Calendar.getInstance();    
		String str = isDate(date) == false ? getMonth(-1) : date;
		Date dt = StringToDate(str, "yyyy-MM-dd");
		gc.setTime(dt);
		gc.add(Calendar.MONTH, i);
		gc.set(Calendar.DAY_OF_MONTH, 1);
		dateStr.append(df.format(gc.getTime())).append(" 00:00:00");
		return dateStr.toString();
	}
	/**
	 * 将时间格式的字符串转换为时间
	 * @param dateStr 时间格式字符串
	 * @param formatStr 格式化字符串
	 * @return 时间
	 */
	/*public static Date StringToDate1(String columnStr, String dateStr,String formatStr){
		DateFormat dd=new SimpleDateFormat(formatStr);
		Date date=null;
		try {
			date = dd.parse(dateStr);
		} catch (ParseException e) {
			log.error("插入oracle错误:"+columnStr+" :" +dateStr+ ":"+formatStr);
			e.printStackTrace();
		}
		return date;
	}*/
	/**
	 * 将时间格式的字符串转换为时间
	 * @param dateStr 时间格式字符串
	 * @param formatStr 格式化字符串
	 * @return 时间
	 */
	public static Date StringToDate(String dateStr,String formatStr){
		DateFormat dd=new SimpleDateFormat(formatStr);
		Date date=null;
		try {
			date = dd.parse(dateStr);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date;
	}
	
	/**
	 * 获取当前时间
	 * @return
	 */
	public static String getCurrentDate(){
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss"); 
		GregorianCalendar gcd = (GregorianCalendar) Calendar.getInstance(); 
		return df.format(gcd.getTime());
	}
	
	/**
	 * 将时间格式的字符串转换为时间字符串
	 * @param dateStr
	 * @param formatStr
	 * @return
	 */
	public static String DateFormatToString(String dateStr, String formatStr){
		String date = dateStr == null || "".equals(dateStr) ? getCurrentDate() : dateStr;
		String format = formatStr == null || "".equals(formatStr) ? "yyyy-MM-dd HH:mm:ss" : formatStr;
		DateFormat df = new SimpleDateFormat(format);
		String timeStr = df.format(StringToDate(date, "yyyy-MM-dd hh:mm:ss"));
		return timeStr;
	}
	/**
	 * 返回这个月的天数
	 * @param date
	 * @return int
	 */
	public static int getDayOfMonth(Date date){
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		int days = cal.getActualMaximum(cal.DATE);
		return days ;
		
	}
	/**
	 * 获取月份中最后一天时间字符串
	 * @param dateStr
	 * @return
	 */
	public static String getMonthLastDay(String dateStr){
		Calendar calendar = Calendar.getInstance(); 
		String date = isDate(dateStr) == false ? getMonth(-1) : dateStr;
    	calendar.setTime(DateUtil.StringToDate(date, "yyyy-MM-dd"));   
		calendar.set(Calendar.DATE, calendar.getActualMaximum(Calendar.DATE));   
		DateFormat format = new SimpleDateFormat("yyyy/MM/dd");   
		String timeStr = format.format(calendar.getTime());
		return timeStr;
	}
	/**
	 * 传入时间字符串 获取当天的开始时间
	 * @param dateStr
	 * @return
	 */
	public static String getStartTimeOfDay(String dateStr){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = null;
		try {
			date = sdf.parse(dateStr);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return sdf1.format(date);
	}
	/**
	 * 传入时间字符串 获取当天开始时间的前七天
	 * @param dateStr
	 * @return
	 */
	public static String getBefore7Day(String dateStr){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = null;
		try {
			date = sdf.parse(dateStr);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.DAY_OF_MONTH, -6);
		date = calendar.getTime();
		return sdf1.format(date);
	}
	/**
	 * 根据传入的时间字符串和前i天(后i天)获取传入的时间前i天(后i天)的时间字符串
	 * @return
	 */
	public static String getDayOfDate(String date, int i){
		StringBuffer dateStr = new StringBuffer();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd"); 
		GregorianCalendar gc = (GregorianCalendar) Calendar.getInstance();    
		String str = isDate(date) == false ? getDay(0) : date;
		Date dt = StringToDate(str, "yyyy-MM-dd");
		gc.setTime(dt);
		if(isDate(date)){
			gc.add(Calendar.DATE, i+1);
		}
		dateStr.append(df.format(gc.getTime())).append(" 00:00:00");
		return dateStr.toString();
	}
	
	/**
	 * 根据传入参数获取当前时间前i天或后i天字符串
	 * @return
	 */
	private static String getDay(int i){
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd"); 
		GregorianCalendar gcd = (GregorianCalendar) Calendar.getInstance(); 
		gcd.add(Calendar.DATE, i+1);
		String date = df.format(gcd.getTime());
		return date;
	}
	
	/**
	 * 传入时间字符串 获取当天的结束时间
	 * @param dateStr
	 * @return
	 */
	public static String getEndTimeOfDay(String dateStr){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = null;
		dateStr = dateStr+" 23:59:59";
		try {
			date = sdf.parse(dateStr);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return sdf.format(date);
	}
	
	public static Map<String, Date> getDateByType(String timeType, String getTime){
		timeType = null == timeType || "".equals(timeType) ? "day" : timeType;
		int i = "day".equals(timeType) ? -1 : -7;
		int j = "day".equals(timeType) ? 0 : -1;
		Date startTime = "day".equals(timeType) || "week".equals(timeType) ?
				DateUtil.StringToDate(DateUtil.getDayOfDate(getTime, i), "yyyy-MM-dd HH:mm:ss")
				:DateUtil.StringToDate(DateUtil.getMonthFirstDay(getTime, 0), "yyyy-MM-dd HH:mm:ss");
		Date endTime = "day".equals(timeType) || "week".equals(timeType) ?
				DateUtil.StringToDate(DateUtil.getDayOfDate(getTime, j), "yyyy-MM-dd HH:mm:ss")
				:DateUtil.StringToDate(DateUtil.getMonthFirstDay(getTime, 1), "yyyy-MM-dd HH:mm:ss");
		DateUtil.dateMap.put("startTime", startTime);		
		DateUtil.dateMap.put("endTime", endTime);		
		return DateUtil.dateMap;
	}
	public static void main(String[] str) {
		/*SimpleDateFormat dateformat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss-SSS");
		Date d1 = getLastMi(60);
		Date d2 = new Date(d1.getTime()-60*60*1000);
		StringBuffer sb = new StringBuffer();
		dateformat.format(d1, sb, new FieldPosition(DateFormat.Field.MILLISECOND));
		sb.append(" ~ ");
		dateformat.format(d2, sb, new FieldPosition(DateFormat.Field.MILLISECOND));
		System.out.println(getMiStr(5));*/
		java.text.SimpleDateFormat   df   =   new   java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");       
//        
//        GregorianCalendar gc = (GregorianCalendar) Calendar.getInstance();    
//        gc.set(2013,0-2, 1);    
//        
//              String day_first = df.format(gc.getTime());   
              
//              System.out.println(day_first);
             
             
//              GregorianCalendar gcd = (GregorianCalendar) Calendar.getInstance(); 
//              System.out.println(df.format(gcd.getTime()));
//              System.out.println(getCurrentMonthFirstDay());
//        System.out.println(df.format(StringToDate("2013-12-1", "yyyy-MM-dd")));
//        System.out.println(StringToDate("2003-12", "yyyy-MM").getMonth()+1);
//        System.out.println("根据传入的时间字符串获取当月或下月的第一天："+getMonthFirstDay("2013-12-3",1));
//        System.out.println("根据传入的时间字符串获取当月或下月的第一天："+getMonthFirstDay("2013-12-3",0));
//        System.out.println(df.format(StringToDate(getMonthFirstDay("2013-12-09",0), "yyyy-MM-dd hh:mm:ss")));
//          System.out.println(isDate("2013-02-01"));
//        DecimalFormat dft = new DecimalFormat("0.00");
//        System.out.println(dft.format(Double.valueOf("0.23")));
//        BigDecimal bg = new BigDecimal(
//        System.out.println(dft.format(new BigDecimal("0")));
//        System.out.println(String.format("%.2f", Double.valueOf(".2")));
//        System.out.println("数值转换："+Double.valueOf("111111111111111111111111111111.2"));
//        System.out.println("数值转换："+new BigDecimal("111111111111111111111111111111.2").add(new BigDecimal("111111111111111111111111111111.2")));
//        
//        System.out.println(getMonth(-1));
//		Date dt = DateUtil.StringToDate(DateUtil.getMonthFirstDay("2014-05-12", -11), "yyyy-MM-dd hh:mm:ss");
//		System.out.println(dt.toString());
//		System.out.println(getStartTimeOfDay("2014-06-12"));
//		System.out.println(getEndTimeOfDay("2014-06-12"));
//		System.out.println(DateUtil.getMonthFirstDay("2014-07-01", -11));
		/*
		//天
		System.out.println(DateUtil.getDayOfDate("2015-04-12", -1));
		System.out.println(DateUtil.getDayOfDate("2015-04-12", 0));
		//周
		System.out.println(DateUtil.getDayOfDate("2015-04-12", -7));
		System.out.println(DateUtil.getDayOfDate("2015-04-12", -1));
		//月
		System.out.println(DateUtil.getMonthFirstDay("2015-04-12", 0));
		System.out.println(DateUtil.getMonthFirstDay("2015-04-12", 1));
		*/
		Map<String, Date> map = DateUtil.getDateByType("week", "2015-04-12");
		System.out.println(df.format(map.get("startTime")));
		System.out.println(df.format(map.get("endTime")));
	}
	
}
