package com.yuanzhy.sqldog.server.core.util;

import org.apache.commons.lang3.time.DateUtils;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Date;

/**
 *
 * @author yuanzhy
 * @date 2018/8/20
 */
public class DateUtil {

    private static final String[] DATE_PATTERNS = {"yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm", "yyyy-MM-dd", "yyyy/MM/dd HH:mm:ss", "yyyyMMddHHmmss"};

    public static Date parse(String dateText) {
        try {
            return DateUtils.parseDate(dateText, DATE_PATTERNS);
        } catch (ParseException e) {
            throw new IllegalArgumentException("解析date参数失败：" + dateText, e);
        }
    }

    public static java.sql.Date parseSqlDate(String dateText) {
        Date d = parse(dateText);
        return new java.sql.Date(d.getTime());
    }

    public static Time parseSqlTime(String dateText) {
        Date d = parse(dateText);
        return new Time(d.getTime());
    }

    public static Timestamp parseTimestamp(String dateText) {
        Date d = parse(dateText);
        return new Timestamp(d.getTime());
    }

    /**
     * 日期是否在区间的边缘，即年月日是否和startTime为同一天或endTime为同一天
     * @param date 待判断
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return
     */
    public static boolean dayInRangeBoundary(Date date, Date startTime, Date endTime) {
        if (startTime == null && endTime == null) {
            return true;
        } else if (startTime == null) {
            return DateUtils.isSameDay(date, endTime);
        } else if (endTime == null) {
            return DateUtils.isSameDay(date, startTime);
        } else {
            return DateUtils.isSameDay(date, startTime) || DateUtils.isSameDay(date, endTime);
        }
    }

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
