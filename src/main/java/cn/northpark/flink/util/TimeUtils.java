
package cn.northpark.flink.util;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;


import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.shaded.guava18.com.google.common.base.Splitter;

/**
 * 时间工具类
 * 
 * @since 2014-05-23
 * @author zhangyang
 *
 */
@Slf4j
public class TimeUtils {
	private static final long ONE_MINUTE = 60000L;
	private static final long ONE_HOUR = 3600000L;
	private static final long ONE_DAY = 86400000L;
	private static final long ONE_WEEK = 604800000L;

	private static final String ONE_SECOND_AGO = "秒前";
	private static final String ONE_MINUTE_AGO = "分钟前";
	private static final String ONE_HOUR_AGO = "小时前";
	private static final String ONE_DAY_AGO = "天前";
	private static final String ONE_MONTH_AGO = "月前";
	private static final String ONE_YEAR_AGO = "年前";

	private static final ImmutableList<String> quarterOf1 = ImmutableList.of("01", "02", "03");
	private static final ImmutableList<String> quarterOf2 = ImmutableList.of("04", "05", "06");
	private static final ImmutableList<String> quarterOf3 = ImmutableList.of("07", "08", "09");
	private static final ImmutableList<String> quarterOf4 = ImmutableList.of("10", "11", "12");

	// 格式化时间串成为 几天前 几秒前 几小时前 几分钟前 几年前sth.....
	public static String formatToNear(String str) {

		try {
			SimpleDateFormat format = null;
			if (str.length() > 10) {
				format = new SimpleDateFormat("yyyy-MM-dd HH:m:s");
			} else {
				format = new SimpleDateFormat("yyyy-MM-dd");
			}
			Date date;
			date = format.parse(str);
			str = TimeUtils.format(date);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
		}

		return str;

	}

	/*
	 * 获取两个8位日期的间隔
	 */
	public static int daysbetween(String beginDate, String endDate) {
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
		int days = 0;
		try {
			if(beginDate!=null&&beginDate.equals(endDate))
				return days;
			Date date = sdf1.parse(endDate);
			Date date2 = sdf1.parse(beginDate);
			days = (int) ((date.getTime() - date2.getTime()) / (24 * 60 * 60 * 1000));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return days;
	}
	
	/**
	 * 获取两个日期之间的间隔
	 * @param date
	 * @return
	 */
	public static BigDecimal unitBetween(String unit,String beginDate, String endDate) {
		
		BigDecimal value = new BigDecimal(0);
		DateTimeFormatter sdf=DateTimeFormatter.ofPattern("yyyyMMdd");
		LocalDate start = LocalDate.parse(beginDate, sdf);
		LocalDate end = LocalDate.parse(endDate, sdf);
	    Period period = Period.between(start, end);
	    int years = period.getYears();
	    int months = period.getMonths();
	    int days = period.getDays();
	    if("Y".equals(unit))
	    {
	    	value=new BigDecimal(years).add(new BigDecimal(months).divide(new BigDecimal(12),6, BigDecimal.ROUND_HALF_UP)).add(new BigDecimal(days).divide(new BigDecimal(365),6, BigDecimal.ROUND_HALF_UP));
	    }
	    else if ("M".equals(unit))
	    {
	    	value=new BigDecimal(months).add(new BigDecimal(years*12)).add(new BigDecimal(days).divide(new BigDecimal(30),6, BigDecimal.ROUND_HALF_UP));
	    }
	    else if ("D".equals(unit))
	    {
	    	value=new BigDecimal(daysbetween(beginDate,endDate));
	    }
	    
	    return value;
	}

	// 格式化时间串成为 几天前 几秒前 几小时前 几分钟前 几年前sth.....
	public static String format(Date date) {
		long delta = new Date().getTime() - date.getTime();
		if (delta < 1L * ONE_MINUTE) {
			long seconds = toSeconds(delta);
			return (seconds <= 0 ? 1 : seconds) + ONE_SECOND_AGO;
		}
		if (delta < 45L * ONE_MINUTE) {
			long minutes = toMinutes(delta);
			return (minutes <= 0 ? 1 : minutes) + ONE_MINUTE_AGO;
		}
		if (delta < 24L * ONE_HOUR) {
			long hours = toHours(delta);
			return (hours <= 0 ? 1 : hours) + ONE_HOUR_AGO;
		}
		if (delta < 48L * ONE_HOUR) {
			return "昨天";
		}
		if (delta < 30L * ONE_DAY) {
			long days = toDays(delta);
			return (days <= 0 ? 1 : days) + ONE_DAY_AGO;
		}
		if (delta < 12L * 4L * ONE_WEEK) {
			long months = toMonths(delta);
			return (months <= 0 ? 1 : months) + ONE_MONTH_AGO;
		} else {
			long years = toYears(delta);
			return (years <= 0 ? 1 : years) + ONE_YEAR_AGO;
		}
	}

	private static long toSeconds(long date) {
		return date / 1000L;
	}

	private static long toMinutes(long date) {
		return toSeconds(date) / 60L;
	}

	private static long toHours(long date) {
		return toMinutes(date) / 60L;
	}

	private static long toDays(long date) {
		return toHours(date) / 24L;
	}

	private static long toMonths(long date) {
		return toDays(date) / 30L;
	}

	private static long toYears(long date) {
		return toMonths(date) / 365L;
	}
	/**
	 * 取得系统当前时间(格式：1970年1月1日0时起到当前的毫秒)
	 *
	 * @return 当前时间
	 */
	public static long getCurrentTime() {
		return System.currentTimeMillis();
	}

	/**
	 * 取得当前时间，格式： yyyy-MM-dd hh:mm:ss
	 *
	 * @return 当前时间
	 */
	public static String getNowTime() {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(new Date().getTime());
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return dateFormat.format(calendar.getTime());
	}

	/**
	 * 取得当前时间，格式： yyyy-MM-dd hh:mm:ss
	 *
	 * @return 当前时间
	 */
	public static String getOnlyTime() {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(new Date().getTime());
		SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
		return dateFormat.format(calendar.getTime());
	}

	/**
	 * 取得英文日期时间，格式： 29 Sep 2011
	 *
	 * @return 当前时间
	 */
	public static String getEnglishDate() {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(new Date().getTime());
		SimpleDateFormat sdf = new SimpleDateFormat("dd MMM yyyy", Locale.ENGLISH);
		return sdf.format(calendar.getTime());
	}

	/**
	 * 转化取得英文日期时间，从 2016-07-22 格式： 29 Sep 2011
	 *
	 * @return 当前时间
	 */
	public static String parse2EnglishDate(String date) {

		SimpleDateFormat sdf = new SimpleDateFormat("dd MMM yyyy", Locale.ENGLISH);
		return sdf.format(stringToMillis(date));
	}

	/**
	 * 字符串(格式：yyyy-MM-dd hh:mm:ss) --> 毫秒(说明：1970年1月1日0时起到当前字符串时间的毫秒)
	 *
	 * @return 毫秒数(1970年1月1日0时起到当前字符串时间的毫秒)
	 */
	public static long stringToMillis(String source) {
		Date date = null;
		SimpleDateFormat dateFormat = null;
		if (source.length() > 20) {
			dateFormat = new SimpleDateFormat("yyyyMMdd HH:mm:ss:SSS");
		} else if (source.length() > 10) {
			dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		} else {
			dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		}
		try {
			date = dateFormat.parse(source);
		} catch (ParseException e) {
			log.error("", e);
			date = new Date();
		}
		if (null == date)
			return 0;
		return date.getTime();
	}

	/**
	 * @author zhangyang 取得当前时间 格式yyyy-MM-dd hh:mm:ss
	 */
	public static String nowTime() {
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(new Date().getTime());
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return dateFormat.format(c.getTime());
	}

	/**
	 * @author zhangyang 取得当前日期 yyyy-MM-dd
	 */
	public static String nowdate() {
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(new Date().getTime());
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		return dateFormat.format(c.getTime());
	}

	/**
	 * @author zhangyang 取得当前年号 yyyy
	 */
	public static String nowYear() {
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(new Date().getTime());
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy");
		return dateFormat.format(c.getTime());
	}

	/**
	 * @author zhangyang 取得当前日期 yyyyMMdd
	 */
	public static String nowNonedate() {
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(new Date().getTime());
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		return dateFormat.format(c.getTime());
	}

	/**
	 * @author zhangyang 取得N个月以后的日期
	 */
	public static String N_MonthDate(int n) {
		Calendar c = Calendar.getInstance();
		c.add(c.MONTH, n);
		return "" + c.get(c.YEAR) + "-" + (c.get(c.MONTH) + 1) + "-" + c.get(c.DATE);
	}


	/**
	 * 取得start日期 N个月以后的日期
	 * @param startDate  yyyyMMdd
	 * @param n
	 * @return
	 */
	public static String N_Month_Date_Start(String startDate, int n) {
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
		try {
			Calendar c = Calendar.getInstance();
			c.setTime(df.parse(startDate));
			c.add(c.MONTH, n);

			return df.format(c.getTime());
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * @author zhangyang 取得N年以后的日期
	 */

	public static String N_YearDate(int n) {
		Calendar c = Calendar.getInstance();
		c.add(c.YEAR, n);
		return "" + c.get(c.YEAR) + "-" + (c.get(c.MONTH) + 1) + "-" + c.get(c.DATE);
	}

	/**
	 * @author zhangyang 取得N年以后的日期
	 */
	public static String N_YearTime(int n) {
		Calendar c = Calendar.getInstance();
		c.add(c.YEAR, n);
		return "" + c.get(c.YEAR) + "-" + (c.get(c.MONTH) + 1) + "-" + c.get(c.DATE) + " " + c.get(c.HOUR) + ":"
				+ c.get(c.MINUTE) + ":" + c.get(c.SECOND);
	}

	/**
	 * @author zhangyang
	 * @function 取得标准时间 2014-05-23从格式yyyy-MM-dd hh:mm:ss
	 */

	public static String getHalfDate(String timeStr) {
		String t = timeStr;
		if (timeStr.contains("-")) {
			t = timeStr.substring(0, 10);
		}
		return t;
	}

	/**
	 * @author zhangyang
	 * @function 取得标准时间 2014从格式yyyy-MM-dd hh:mm:ss
	 */
	public static String getYear(String timeStr) {
		String t = timeStr;
		if (timeStr.contains("-")||timeStr.length()>4) {
			t = timeStr.substring(0, 4);
		}
		return t;
	}

	/**
	 * 获取月份
	 * 
	 * @param time
	 *            标准时间字符串（yyyy-MM-dd hh:mm:ss）
	 * @return 月份
	 */
	public static String getMonth(String time) {
		if (null == time)
			return null;
		if (time.contains("-")) {
			return time.substring(5, 7);
		}
		return null;
	}

	/**
	 * @author zhangyang
	 * @function 取得标准时间 2014从格式yyyy-MM-dd hh:mm:ss
	 */

	public static String getDay(String timeStr) {
		String t = timeStr;
		if (timeStr.contains("-")) {
			String[] str = timeStr.split("-");
			if (str.length > 2) {
				t = str[2];
			}
		}
		return t;
	}

	/**
	 * @author zhangyang
	 * @function 计算两个时间的差：耗时多少
	 * @param --例子"2004-01-02
	 *            11:30:24"
	 * @throws ParseException
	 */

	public static String getPastTime(String nowtime, String oldtime) throws ParseException {//
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date now = df.parse(nowtime);
		Date date = df.parse(oldtime);
		long l = now.getTime() - date.getTime();
		long day = l / (24 * 60 * 60 * 1000);
		long hour = (l / (60 * 60 * 1000) - day * 24);
		long min = ((l / (60 * 1000)) - day * 24 * 60 - hour * 60);
		long s = (l / 1000 - day * 24 * 60 * 60 - hour * 60 * 60 - min * 60);
		String pastTime = "" + day + "天" + hour + "小时" + min + "分" + s + "秒";

		return pastTime;
	}

	/**
	 * @author zhangyang
	 * @function 计算两个时间天数
	 * @throws ParseException
	 */

	public static BigDecimal getPastDay(String nowtime, String oldtime) throws ParseException {//
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
		Date now = df.parse(nowtime);
		Date date = df.parse(oldtime);
		long l = now.getTime() - date.getTime();
		long day = l / (24 * 60 * 60 * 1000);
		BigDecimal pastTime = new BigDecimal(day);
		return pastTime;
	}

	/**
	 * @author zhangyang
	 * @function 计算是否过期,true未过期；false过期
	 * @throws ParseException
	 */

	public static boolean isInvalid(String nowtime, String overtime) throws ParseException {//
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date now = df.parse(nowtime);
		Date over = df.parse(overtime);
		long l = now.getTime() - over.getTime();
		boolean flag = true;
		if (l > 0) {
			flag = false;
		} else {
			flag = true;
		}
		return flag;
	}

	/**
	 * @desc 取得前一天的时间
	 * @param specifiedDay
	 * @return
	 */
	public static String getDayBefore(String specifiedDay) {
		// SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
		Calendar c = Calendar.getInstance();
		Date date = null;
		try {
			date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(specifiedDay);
		} catch (ParseException e) {
			log.error("", e);
		}
		c.setTime(date);
		c.add(Calendar.DATE,-1);

		String dayBefore = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(c.getTime());
		return dayBefore;
	}

	/**
	 * @desc 取得后一天的时间
	 * @param specifiedDay
	 * @return
	 */
	public static String getDayAfter(String specifiedDay) {
		Calendar c = Calendar.getInstance();
		Date date = null;
		try {
			date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(specifiedDay);
		} catch (ParseException e) {
			log.error("", e);
		}
		c.setTime(date);
		c.add(Calendar.DATE,+1);

		String dayAfter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(c.getTime());
		return dayAfter;
	}

	/**
	 * @author zhangyang
	 * @desc yyyy-MM-dd HH:mm:ss 转换为 yyyyMMdd
	 * @return
	 * @throws ParseException
	 */
	public static String StringToEight(String time) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date myDate2 = dateFormat2.parse(time);
		return sdf.format(myDate2);
	}

	/**
	 * @author zhangyang
	 * @desc 取得前后N天的时间,N=正负数
	 * @param specifiedDay
	 * @return
	 */
	public static String getDayAfterOrBeforeN(String specifiedDay, int N) {
		Calendar c = Calendar.getInstance();
		Date date = null;
		try {
			date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(specifiedDay);
		} catch (ParseException e) {
			log.error("", e);
		}
		c.setTime(date);
		c.add(Calendar.DATE,  N);

		String dayAfter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(c.getTime());
		return dayAfter;
	}

	/**
	 * @author zhangyang
	 * @desc 取得前后N分钟后的时间,N=正负数
	 * @param specifiedDay
	 * @return
	 */
	public static String getMinuteAfterOrBeforeN(String specifiedDay, int N) {
		Calendar c = Calendar.getInstance();
		Date date = null;
		try {
			date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(specifiedDay);
		} catch (ParseException e) {
			log.error("", e);
		}
		c.setTime(date);
		c.add(Calendar.MINUTE,    N);

		String minuteAfter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(c.getTime());
		return minuteAfter;
	}



	/**
	 * @author zhangyang
	 * @desc 取得当前的年月比如： 1407
	 * @param specifiedDay
	 * @return
	 */
	public static String getYearMonth() {
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(new Date().getTime());
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyMM");
		return dateFormat.format(c.getTime());
	}

	/**
	 * @author zhangyang
	 * @desc 把long转化成String 将长时间格式字符串转换为字符串 yyyy-MM-dd HH:mm:ss
	 * @return
	 */
	public static String longToString(Long time) {
		Date date = new Date(time);
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String dateString = formatter.format(date);
		// System.out.println("TIME:::"+dateString);
		return dateString;
	}

	/**
	 * @author zhangyang
	 * @desc 把20190619时分秒 转化成Date()
	 * @return
	 * @throws ParseException
	 */
	public static Date NumberToDate(String time) throws ParseException {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHH:mm:ss");
		return formatter.parse(time);
	}

	/**
	 * @author zhangyang
	 * @desc 把long转化成String 将长时间格式字符串转换为字符串 yyyy-MM-dd HH:mm:ss
	 * @return
	 */
	public static String longToDateStrng(Long time) {
		Date date = new Date(time);
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		String dateString = formatter.format(date);
		System.out.println("TIME:::" + dateString);
		return dateString;
	}

	/**
	 * @desc 随机年07-15
	 * @return
	 */
	public static String getRandomYear() {

		String a = "";
		Random ramdom = new Random();
		int number = -1;
		int max = 15;

		// size 为 10 ，取得类似0-9的区间数
		number = Math.abs(ramdom.nextInt() % max) + 7;

		if (String.valueOf(number).length() == 1) {
			a = "20" + 0 + "" + number;
		} else {
			a = "20" + number + "";
		}

		return a;

	}

	/**
	 * @desc 随机月01-12
	 * @return
	 */
	public static String getRandomMonth() {

		String a = "";
		Random ramdom = new Random();
		int number = -1;
		int max = 12;

		// size 为 10 ，取得类似0-9的区间数
		number = Math.abs(ramdom.nextInt() % max);
		if (String.valueOf(number).length() == 1) {
			a = 0 + "" + number;
		} else {
			a = number + "";
		}

		return a;

	}

	/**
	 * @desc 随机日1-31
	 * @return
	 */
	public static String getRandomDay() {

		String a = "";
		Random ramdom = new Random();
		int number = -1;
		int max = 30;

		// size 为 10 ，取得类似0-9的区间数
		number = Math.abs(ramdom.nextInt() % max);
		if (String.valueOf(number).length() == 1) {
			a = 0 + "" + number;
		} else {
			a = number + "";
		}
		return a;

	}

	/**
	 * @desc 随机日期从2007-至今
	 * @return
	 */
	public static String getRandomDate() {

		return getRandomYear() + "-" + getRandomMonth() + "-" + getRandomDay();

	}

	/**
	 * @author zhangyang 获取N个工作日以后的日期
	 * @param currentDate
	 * @param days
	 * @return
	 */
	public static Date getWorkDate(Date currentDate, int days) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(currentDate);
		int i = 0;
		while (i < days) {
			calendar.add(Calendar.DATE, 1);
			i++;
			if (calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY
					|| calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
				i--;
			}
		}
		return calendar.getTime();
	}

	/**
	 * @author zhangyang 获取N个工作日以前的日期
	 * @param currentDate
	 * @param days
	 * @return
	 */
	public static Date getWorkDateBefore(Date currentDate, int days) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(currentDate);
		int i = 0;
		while (i < days) {
			calendar.add(Calendar.DATE, -1);
			i++;
			if (calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY
					|| calendar.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
				i--;
			}
		}
		return calendar.getTime();
	}

	/**
	 * @author zhangyang 获取从今天N个工作日以后的具体时间
	 * @param days
	 * @return
	 */
	public static String getWorkDateTime(int days) {
		Date Ndate = getWorkDate(new Date(), days);
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return dateFormat.format(Ndate);
	}

	public static String Ten2Two(int number) {
		String result = "";
		int sum = 0;
		for (int i = number; i >= 1; i /= 2) {

			if (i % 2 == 0) {
				sum = 0;
			} else {
				sum = 1;
			}
			result = sum + result;

		}
		System.out.print(result);
		return result;
	}

	/**
	 * @author zhangyang 获取没有下划线的日期
	 * @like 从2014-06-06 转为20140606
	 * @return
	 * @throws ParseException
	 */
	public static String getNone_Date(String date) throws ParseException {
		if (!date.contains("-")) {
			return date;
		}
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date d = dateFormat.parse(date);
		SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyyMMdd");
		return dateFormat2.format(d);
	}

	/**
	 * @author zhangyang 获取没有下划线的日期
	 * @like 从2014-06-06 转为20140606
	 * @return
	 * @throws ParseException
	 */
	public static BigDecimal getNoneDateByDate(Date date) throws ParseException {
		String dates = String.valueOf(date);
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date d = dateFormat.parse(dates);
		SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyyMMdd");
		return new BigDecimal(dateFormat2.format(d));
	}

	/**
	 * @author zhangyang 获取有下划线的日期
	 * @like 从20140606 转为 2014-06-06
	 * @return
	 * @throws ParseException
	 */
	public static String getHave_Date(String date) throws ParseException {
		if (date.contains("-")) {
			return date;
		}
		SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyyMMdd");
		Date d = dateFormat2.parse(date);
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		return dateFormat.format(d);
	}

	/**
	 * 描述：得到一个唯一的数字主键
	 *
	 * @return
	 */
	public static synchronized String getUniqueKey() {
		Random random = new Random();
		Integer number = random.nextInt(900000) + 100000;
		String str = System.currentTimeMillis() + String.valueOf(number);
		return str.substring(str.length() - 10);
	}


	/**
	 * @author w_zhangyang 判断时间是否在时间段内
	 * @param repxTimeType
	 *            ： 08:00 - 18:00
	 * @param mytime
	 *            ： 12:00
	 * @return
	 */
	public static boolean belongCalendar(String repxTimeType, String mytime) {

		SimpleDateFormat df = new SimpleDateFormat("HH:mm");// 设置日期格式
		Date now = null;
		Date beginTime = null;
		Date endTime = null;

		List<String> times = Splitter.onPattern("[-]").omitEmptyStrings().splitToList(repxTimeType);

		try {
			now = df.parse(mytime);
			if (CollectionUtils.isNotEmpty(times)) {
				beginTime = df.parse(times.get(0));
				endTime = df.parse(times.get(1));
			}
		} catch (Exception e) {
			log.error("", e);
		}

		return belongCalendar(now, beginTime, endTime);
	}

	/**
	 * 判断时间是否在时间段内
	 * 
	 * @param nowTime
	 * @param beginTime
	 * @param endTime
	 * @return
	 */
	public static boolean belongCalendar(Date nowTime, Date beginTime, Date endTime) {
		Calendar date = Calendar.getInstance();
		date.setTime(nowTime);

		Calendar begin = Calendar.getInstance();
		begin.setTime(beginTime);

		Calendar end = Calendar.getInstance();
		end.setTime(endTime);

		if (date.after(begin) && date.before(end)) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 
	 * 功能: 判断是否是月末
	 * 
	 * @param mytime
	 * @return true月末,false不是月末
	 */
	public static boolean isMonthLastDay(String mytime) {
		try {
			SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");// 设置日期格式
			Calendar cal = Calendar.getInstance();
			cal.setTime(df.parse(mytime));
			if (cal.get(Calendar.DATE) == cal.getActualMaximum(Calendar.DAY_OF_MONTH))
				return true;
			else
				return false;
		} catch (Exception e) {
			// TODO: handle exception
			return false;
		}

	}

	/**
	 * 
	 * 功能: 判断是否是月初
	 * 
	 * @param mytime
	 * @return true月末,false不是月末
	 */
	public static boolean isMonthFirstDay(String mytime) {
		try {
			SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");// 设置日期格式
			Calendar cal = Calendar.getInstance();
			cal.setTime(df.parse(mytime));
			if (cal.get(Calendar.DATE) == cal.getActualMinimum(Calendar.DAY_OF_MONTH))
				return true;
			else
				return false;
		} catch (Exception e) {
			// TODO: handle exception
			return false;
		}

	}

	public static String getQuarterNum(String quarter) {
		try {

			SimpleDateFormat date_sdf = new SimpleDateFormat("yyyyMMdd");

			Date date = date_sdf.parse(quarter);

			SimpleDateFormat month_sdf = new SimpleDateFormat("MM");
			String month = month_sdf.format(date);

			if (quarterOf1.contains(month)) {
				return "1";
			} else if (quarterOf2.contains(month)) {
				return "2";
			} else if (quarterOf3.contains(month)) {
				return "3";
			} else if (quarterOf4.contains(month)) {
				return "4";
			}
		} catch (Exception e) {
			// TODO: handle exception
			log.error("", e);
		}

		return null;
		// TODO Auto-generated method stub

	}

	/**
	 * @author zhangyang
	 * @desc 取得前后N天的时间,N=正负数
	 * @param specifiedDay [ yyyyMMdd]
	 * @return
	 */
	public static String getDayAfterBeforeN(String specifiedDay, int N) {
		Calendar c = Calendar.getInstance();
		Date date = null;
		try {
			date = new SimpleDateFormat("yyyyMMdd").parse(specifiedDay);
		} catch (ParseException e) {
			log.error("", e);
		}
		c.setTime(date);
		c.add(Calendar.DATE,  N);

		String dayAfter = new SimpleDateFormat("yyyyMMdd").format(c.getTime());
		return dayAfter;
	}

	public static String getCurrentQuarter() {
		Calendar cal = Calendar.getInstance();
		int m = cal.get(Calendar.MONTH) + 1;
		String quarter = " ";
		if (m >= 1 && m == 3) {
			quarter = "1";
		}
		if (m >= 4 && m <= 6) {
			quarter = "2";
		}
		if (m >= 7 && m <= 9) {
			quarter = "3";
		}
		if (m >= 10 && m <= 12) {
			quarter = "4";
		}
		return quarter;
	}

	/**
	 * 功能: 将时间类型转换为mysql 时间格式
	 * 
	 * @param date
	 * @return mysql 查询时间格式
	 */
	public static String getDateBaseTimeFormat(String date) { // mysql 时间格式
		String datebaseFormat = "";
		/*
		 * if(date.contains("-")){
		 * datebaseFormat="str_to_date('"+date+"','%Y-%m-%d')"; }else
		 * if(date.contains("/")){
		 * datebaseFormat="str_to_date('"+date+"','%Y/%m/%d')"; }else{
		 * datebaseFormat="str_to_date('"+date+"','%Y%m%d')"; }
		 */

		if (date.contains("-")) {
			if (date.length() > 10) {
				datebaseFormat = date.substring(0, 10);
			} else {
				datebaseFormat = date;
			}
			datebaseFormat = "to_date('" + datebaseFormat + "','yyyy-mm-dd')";
		} else if (date.contains("/")) {
			if (date.length() > 10) {
				datebaseFormat = date.substring(0, 10);
			} else {
				datebaseFormat = date;
			}
			datebaseFormat = "to_date('" + datebaseFormat + "','yyyy/mm/dd')";
		} else {
			datebaseFormat = "to_date('" + date + "','yyyymmdd')";
		}
		return datebaseFormat;
	}


	/**
	 * 功能: 获取当前日期
	 * 
	 * @param
	 * @return
	 */
	public static String getFormatNowDate() {
		String date = "to_timestamp('" + new Timestamp(System.currentTimeMillis()) + "','yyyy-mm-dd HH24:mi:ss.Ff')";

		return date;
	}

	/**
	 * 功能: 获取当前日期
	 * 
	 * @param
	 * @return
	 */
	public static String getFormatToDate() {
		String date = "to_date('" + new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new Date())
				+ "','yyyy/mm/dd hh24:mi:ss')";

		return date;
	}


	/**
	 * 判断一个事件是否是date类型(yyyyMMDD/yyyy-MM-dd)
	 * 
	 * @param date
	 * @return
	 */
	public static boolean isFormatDate(String date) {
		try {
			date = date.replace("-", "");
			new SimpleDateFormat("yyyyMMdd").parse(date);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * 月份去除0
	 * 
	 * @param month
	 * @return
	 */
	public static String trimZeroStart(String month) {
		// TODO Auto-generated method stub
		if (month.startsWith("0")) {
			return month.replace("0", "");
		}
		return month.replace("0", "");
	}

	// 本月初
	public static String getMonthFirstDay(String date) {
		String firstMonthDay = date.substring(0, 6);
		firstMonthDay = firstMonthDay + "01";
		return firstMonthDay;
	}

	// 本年初
	public static String getYearFirstDay(String date) {
		String firstYearDay = date.substring(0, 4);
		firstYearDay = firstYearDay + "0101";
		return firstYearDay;
	}

	// 本季度初
	public static String getQuarterFirstDay(String date) {
		String year = date.substring(0, 4);
		int month = Integer.valueOf(date.substring(4, 6));
		String quarter = null;
		switch (month) {
		case 1:
		case 2:
		case 3:
			quarter = "01";
			break;
		case 4:
		case 5:
		case 6:
			quarter = "04";
			break;
		case 7:
		case 8:
		case 9:
			quarter = "07";
			break;
		case 10:
		case 11:
		case 12:
			quarter = "10";
			break;
		default:
			break;
		}
		String quarterFirstDay = year + quarter + "01";
		return quarterFirstDay;
	}

	// 本季度末
	public static String getQuarterEndDay(String date) {
		String year = date.substring(0, 4);
		int month = Integer.valueOf(date.substring(4, 6));
		String  day = "";
		String quarter = null;
		switch (month) {
		case 1:
		case 2:
		case 3:
			quarter = "03";
			day="31";
			break;
		case 4:
		case 5:
		case 6:
			quarter = "06";
			day="30";
			break;
		case 7:
		case 8:
		case 9:
			quarter = "09";
			day="30";
			break;
		case 10:
		case 11:
		case 12:
			quarter = "12";
			day="31";
			break;
		default:
			break;
		}
		String quarterFirstDay = year + quarter + day;
		return quarterFirstDay;
	}
	
	// 上季度末
	public static String getPreviousQuarterLastDay(String date) {
		String year = date.substring(0, 4);
		int   month = Integer.valueOf(date.substring(4, 6));
		String  day = "";
		String quarter = null;
		switch (month) {
			case 1:
			case 2:
			case 3:
				quarter = "12";
				year = String.valueOf(Integer.parseInt(year) -1 );
				day = "31";
				break;
			case 4:
			case 5:
			case 6:
				quarter = "03";
				day = "31";
				break;
			case 7:
			case 8:
			case 9:
				quarter = "06";
				day = "30";
				break;
			case 10:
			case 11:
			case 12:
				quarter = "9";
				day = "30";
				break;
			default:
				break;
		}
		String previousQuarterLastDay = year + quarter + day;
		return previousQuarterLastDay;
	}

	// 上月末
	public static String getLastMonthEndDay(String date) {
		String firstMonthDay = date.substring(0, 6);
		String lastMonthDay = getLastDay(firstMonthDay + "01");
		return lastMonthDay;
	}


	// 上季末
	public static String getLastQuarterEndDay(String date) {
		String year = date.substring(0, 4);
		int month = Integer.valueOf(date.substring(4, 6));
		String quarter = null;
		switch (month) {
		case 1:
		case 2:
		case 3:
			quarter = "01";
			break;
		case 4:
		case 5:
		case 6:
			quarter = "04";
			break;
		case 7:
		case 8:
		case 9:
			quarter = "07";
			break;
		case 10:
		case 11:
		case 12:
			quarter = "10";
			break;
		default:
			break;
		}
		String lastMonthDay = getLastDay(year + quarter + "01");
		return lastMonthDay;
	}
	
	// 上季度初
	public static String getLastQuarterFirstDay(String date) {
		String year = date.substring(0, 4);
		int month = Integer.valueOf(date.substring(4, 6));
		String quarter = null;
		switch (month) {
		case 1:
		case 2:
		case 3:
			quarter = "10";
			year = String.valueOf(Integer.parseInt(year) -1 );
			break;
		case 4:
		case 5:
		case 6:
			quarter = "01";
			break;
		case 7:
		case 8:
		case 9:
			quarter = "04";
			break;
		case 10:
		case 11:
		case 12:
			quarter = "07";
			break;
		default:
			break;
		}
		String previousQuarterFirstDay = year + quarter + "01";
		return previousQuarterFirstDay;
	}

	// 上年末
	public static String getLastYearEndDay(String date) {
		int year = Integer.valueOf(date.substring(0, 4));
		String monthDay = "1231";
		year = year - 1;
		String lastYearDay = year + monthDay;
		return lastYearDay;
	}

	// 获取上一天
	public static String getLastDay(String preDate) {
		Calendar c = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Date date = null;
		try {
			date = sdf.parse(preDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		c.setTime(date);
		c.add(Calendar.DATE, -1);

		return sdf.format(c.getTime());
	}

	// 获取上月初
	public static String getLastMonthFirstDay(String preDate) {
		Calendar c = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Date date = null;
		try {
			date = sdf.parse(preDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		c.setTime(date);
		c.add(Calendar.MONTH, -1);
		c.set(Calendar.DAY_OF_MONTH, 1);

		return sdf.format(c.getTime());
	}

	// 获取下一天
	public static String getNextDay(String preDate) {
		Calendar c = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Date date = null;
		try {
			date = sdf.parse(preDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}

		c.setTime(date);
		c.add(Calendar.DATE, +1);

		return sdf.format(c.getTime());
	}

	// 日期格式转换
	public static String dateformat(String date) throws Exception {
		String formatDate = null;
		if (date != null && isFormatDate(date)) {
			Date newDate = new SimpleDateFormat("yyyyMMdd").parse(date.replace("-", ""));
			formatDate = new SimpleDateFormat("yyyy-MM-dd").format(newDate);
		}
		return formatDate;
	}


	/**
	 * 获取两个日期之间的相隔天数 例:2019-11-11,2019-11-13
	 * 
	 * @param cntDateBeg
	 * @param cntDateEnd
	 * @return
	 */
	public static List<String> addDates(String cntDateBeg, String cntDateEnd) {
		List<String> list = new ArrayList<>();
		String[] dateBegs = cntDateBeg.split("-");
		String[] dateEnds = cntDateEnd.split("-");
		Calendar start = Calendar.getInstance();
		start.set(Integer.valueOf(dateBegs[0]), Integer.valueOf(dateBegs[1]) - 1, Integer.valueOf(dateBegs[2]));
		Long startTIme = start.getTimeInMillis();
		Calendar end = Calendar.getInstance();
		end.set(Integer.valueOf(dateEnds[0]), Integer.valueOf(dateEnds[1]) - 1, Integer.valueOf(dateEnds[2]));
		Long endTime = end.getTimeInMillis();
		Long oneDay = 1000 * 60 * 60 * 24l;
		Long time = startTIme;
		while (time <= endTime) {
			Date d = new Date(time);
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			time += oneDay;
			list.add(df.format(d));
		}
		return list;
	}

	/*
	 *星期几 
	 */
	public static int getDateWeek(String lDate)
	{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Calendar cal = Calendar.getInstance();
		int dayWeek=1;
		try
		{
			Date date = sdf.parse(lDate);
			cal.setTime(date);
			dayWeek = cal.get(Calendar.DAY_OF_WEEK) -1 ;// 获得当前日期是一个星期的第几天

			if(dayWeek==0){
				return 7;//星期天
			}
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return dayWeek;

	}
	// 得到参数这周最后一天
	public static String getWeekLastDate(String afterCheckPosDate) throws Exception {
		String lastDay = "";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Calendar cal = Calendar.getInstance();
		Date date = sdf.parse(afterCheckPosDate);
		cal.setTime(date);
		int dayWeek = cal.get(Calendar.DAY_OF_WEEK);// 获得当前日期是一个星期的第几天
		if (1 == dayWeek) {
			cal.add(Calendar.DAY_OF_MONTH, -1);
		}
		cal.setFirstDayOfWeek(Calendar.MONDAY);
		int day = cal.get(Calendar.DAY_OF_WEEK);
		cal.add(Calendar.DATE, cal.getFirstDayOfWeek() - day);
//		String imptimeBegin = sdf.format(cal.getTime()); // 周一
		cal.add(Calendar.DATE, 6);
		lastDay = sdf.format(cal.getTime()); // 周日
		return lastDay;
	}

    public static String getWeekMonday(String lDate) {

		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");

		String rs = null;
		try {
			Date dates = format.parse(lDate);
			Calendar cal = Calendar.getInstance(Locale.CHINA);
			cal.setTime(dates);


			int dayWeek = cal.get(Calendar.DAY_OF_WEEK);// 获得当前日期是一个星期的第几天
			if (1 == dayWeek) {
				cal.add(Calendar.DAY_OF_MONTH, -1);
			}
			cal.setFirstDayOfWeek(Calendar.MONDAY);

			int day = cal.get(Calendar.DAY_OF_WEEK);
			cal.add(Calendar.DATE, cal.getFirstDayOfWeek() - day);


			rs = format.format(cal.getTime());
			System.out.println("周一日期:" + rs);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return rs;

    }


    public static String getWeekFirstDay(String lDate) {

		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");

		String rs = null;
		try {
			Date dates = format.parse(lDate);
			Calendar cal = Calendar.getInstance(Locale.CHINA);
			cal.setTime(dates);

			int dayWeek = cal.get(Calendar.DAY_OF_WEEK);// 获得当前日期是一个星期的第几天
			System.out.println(dayWeek);
			int day = cal.get(Calendar.DAY_OF_WEEK);
			cal.add(Calendar.DATE, cal.getFirstDayOfWeek() - day-1);


			rs = format.format(cal.getTime());
			System.out.println(rs);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return rs;

    }
    
    public static String getLastFriday(String lDate) {

		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");

		String rs = null;
		try {
			Date dates = format.parse(lDate);
			Calendar cal = Calendar.getInstance(Locale.CHINA);
			cal.setTime(dates);
			int day = cal.get(Calendar.DAY_OF_WEEK);
			cal.add(Calendar.DATE, cal.getFirstDayOfWeek() - day-2);


			rs = format.format(cal.getTime());
			System.out.println(rs);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return rs;

    }
    
    
    public static String getLastWeekEnd(String lDate) {

		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");

		String rs = null;
		try {
			Date dates = format.parse(lDate);
			Calendar cal = Calendar.getInstance(Locale.CHINA);
			cal.setTime(dates);

			int day = cal.get(Calendar.DAY_OF_WEEK);
			System.out.println(day);
			cal.add(Calendar.DATE, cal.getFirstDayOfWeek() - day);


			rs = format.format(cal.getTime());
			System.out.println(rs);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return rs;

    }
    
    /**
     * 本年以来第几天
     */
    public static int getDayYear(String lDate) {

		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		int dayYear=1;
		try {
			Date dates = format.parse(lDate);
			Calendar cal = Calendar.getInstance(Locale.CHINA);
			cal.setTime(dates);

			dayYear = cal.get(Calendar.DAY_OF_YEAR);// 获得当前日期是一个星期的第几天
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return dayYear;

    }

	/**
	 * 获取去年这一天
	 */
	public static String getSameDayLastYear(String lDate) {

		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		try {
			Date dates = format.parse(lDate);
			Calendar cal = Calendar.getInstance(Locale.CHINA);
			cal.setTime(dates);
			cal.add(Calendar.YEAR,-1);

			String rs = format.format(cal.getTime());
			return rs;

		} catch (ParseException e) {
			e.printStackTrace();
		}

		return null;

	}

	/**
	 * 获取天 yyyyMMdd
	 * @param lDate
	 * @return
	 */
	public static int getDateDay(String lDate) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		Calendar cal = Calendar.getInstance();
		int day = 1;
		try {
			Date date = sdf.parse(lDate);
			cal.setTime(date);
			day = cal.get(Calendar.DATE);// 获得当前日期是一个星期的第几天

		} catch (Exception e) {
			e.printStackTrace();
		}
		return day;

	}

	/**
	 * 把log4j格式的时间转化为正常时间
	 * @author zhangyang
	 * @param actionTime
	 * @return
	 * @throws ParseException
	 */
	public synchronized static String TLogTime2NoramTime(String actionTime) throws ParseException{
		SimpleDateFormat dff = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS",Locale.ENGLISH);//输入的被转化的时间格式
		SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//需要转化成的时间格式

		Date date1 = dff.parse(actionTime);

		return  df1.format(date1);

	}
	


	/**
	 * 获取月份的最后一天
	 * @param year
	 * @param month
	 * @return
	 */
	public static String getLastDayOfMonth(String year, String month) {
		Calendar cal = Calendar.getInstance();
		//年
		cal.set(Calendar.YEAR, Integer.parseInt(year));
		//月，因为Calendar里的月是从0开始，所以要-1
		cal.set(Calendar.MONTH, Integer.parseInt(month) - 1);
		//日，设为一号
		cal.set(Calendar.DATE, 1);
		//月份加一，得到下个月的一号
		cal.add(Calendar.MONTH, 1);
		//下一个月减一为本月最后一天
		cal.add(Calendar.DATE, -1);
		return String.valueOf(cal.get(Calendar.DAY_OF_MONTH));//获得月末是几号
	}

	/**
	 * 获取本月最后一天 YYYYMMDD
	 * @return
	 */
	public static String getLastDayOfMonth() {

		// 获取当前年份、月份、日期
		Calendar cale = Calendar.getInstance();
		int year = cale.get(Calendar.YEAR);
		int month = cale.get(Calendar.MONTH) + 1;
		int day = cale.get(Calendar.DATE);
		int hour = cale.get(Calendar.HOUR_OF_DAY);
		int minute = cale.get(Calendar.MINUTE);
		int second = cale.get(Calendar.SECOND);
		int dow = cale.get(Calendar.DAY_OF_WEEK);
		int dom = cale.get(Calendar.DAY_OF_MONTH);
		int doy = cale.get(Calendar.DAY_OF_YEAR);
		// 获取当月第一天和最后一天
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		String lastday;


		// 获取当月的最后一天
		cale = Calendar.getInstance();
		cale.add(Calendar.MONTH, 1);
		cale.set(Calendar.DAY_OF_MONTH, 0);
		lastday = format.format(cale.getTime());
		System.out.println("本月最后一天是 ： " + lastday);


		return  lastday;
	}

	public static void main(String[] args) throws  Exception{
//		String sameDayLastYear = getSameDayLastYear("20200527");
//		System.out.println(sameDayLastYear);
//		String yearFirstDay = getYearFirstDay("20200527");
//		System.out.println(yearFirstDay);
//		int dateDay = getDateDay("20200505");
//		System.out.println("dateDay---------"+dateDay);

//		String tLogTime2NoramTime = TLogTime2NoramTime("2020-08-11T18:17:05.089");


//		int dateWeek = getDateWeek("20200825");

//		String s = getWeekFirstDay("20210409");
//		String s = getNextDay("20210101");
//		String s1 = getNextDay("20201231");
//		String s2 = getNextDay("20200101");

//		System.err.println(s1);
//		System.err.println(s2);


//		for (int i = 1; i < 12; i++) {
//			System.err.println(getLastDayOfMonth("2021",  i+""));
//		}


		System.out.println(isFormatDate("2021-4-6"));

	}


}
