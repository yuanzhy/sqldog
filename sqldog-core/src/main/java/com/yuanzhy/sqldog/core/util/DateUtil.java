package com.yuanzhy.sqldog.core.util;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author yuanzhy
 * @date 2018/8/20
 */
public class DateUtil {

    private static final String DATE_PATTERN = "yyyy-MM-dd";
    private static final String TIME_PATTERN = "HH:mm:ss";
    private static final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

    public static Date parse(String dateText, String... patterns) {
        for (String pattern : patterns) {
            try {
                Date d = new SimpleDateFormat(pattern).parse(dateText);
                if (d != null) {
                    return d;
                }
            } catch (ParseException e) {
                continue;
            }
        }
        throw new RuntimeException("unknown date: " + dateText);
    }

    public static java.sql.Date parseSqlDate(String dateText) {
        Date d = parse(dateText, DATE_PATTERN, DATETIME_PATTERN);
        return new java.sql.Date(d.getTime());
    }

    public static Time parseSqlTime(String dateText) {
        Date d = parse(dateText, TIME_PATTERN);
        return new Time(d.getTime());
    }

    public static java.util.Date parseDatetime(String dateText) {
         return parse(dateText, TIME_PATTERN);
    }

    public static Timestamp parseTimestamp(String dateText) {
        Date d = parse(dateText, DATETIME_PATTERN, DATE_PATTERN);
        return new Timestamp(d.getTime());
    }

    public static String formatSqlDate(java.sql.Date date) {
        return new SimpleDateFormat(DATE_PATTERN).format(date);
    }

    public static String formatTime(Time time) {
        return new SimpleDateFormat(TIME_PATTERN).format(time);
    }

    public static String formatTimestamp(Timestamp timestamp) {
        return new SimpleDateFormat(DATETIME_PATTERN).format(timestamp);
    }

    public static String formatDatetime(Date date) {
        return new SimpleDateFormat(DATETIME_PATTERN).format(date);
    }
//
//    /**
//     * 日期是否在区间的边缘，即年月日是否和startTime为同一天或endTime为同一天
//     * @param date 待判断
//     * @param startTime 开始时间
//     * @param endTime 结束时间
//     * @return
//     */
//    public static boolean dayInRangeBoundary(Date date, Date startTime, Date endTime) {
//        if (startTime == null && endTime == null) {
//            return true;
//        } else if (startTime == null) {
//            return DateUtils.isSameDay(date, endTime);
//        } else if (endTime == null) {
//            return DateUtils.isSameDay(date, startTime);
//        } else {
//            return DateUtils.isSameDay(date, startTime) || DateUtils.isSameDay(date, endTime);
//        }
//    }

    /**
     * 日期时间是否在区间内
     * @param date 待判断
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return
     */
    public static  boolean datetimeInRange(Date date, Date startTime, Date endTime) {
        if (startTime == null && endTime == null) {
            return true;
        } else if (startTime == null) {
            return date.getTime() <= endTime.getTime();
        } else if (endTime == null) {
            return date.getTime() >= startTime.getTime();
        } else {
            return date.getTime() >= startTime.getTime() && date.getTime() <= endTime.getTime();
        }
    }
}
