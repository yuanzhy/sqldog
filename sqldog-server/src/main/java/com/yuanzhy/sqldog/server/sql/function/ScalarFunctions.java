package com.yuanzhy.sqldog.server.sql.function;

import com.yuanzhy.sqldog.core.util.Asserts;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/27
 */
public class ScalarFunctions {

//    public static final SqlFunction NOW = new SqlFunction("NOW",
//            SqlKind.OTHER_FUNCTION,
//            ReturnTypes.TIMESTAMP,
//            null,
//            null,
//            SqlFunctionCategory.TIMEDATE);

    // 日期相关 -----------------------------------------------------------------------
    public static final Timestamp now() {
        return new Timestamp(System.currentTimeMillis());
    }

    public static final Date current_date() {
        return new Date(System.currentTimeMillis());
    }

    public static final Time current_time() {
        return new Time(System.currentTimeMillis());
    }

    public static final Timestamp current_timestamp() {
        return new Timestamp(System.currentTimeMillis());
    }

    public static Map<String, Integer> dateFieldType = new HashMap<String, Integer>() {{
        put("year", Calendar.YEAR);
        put("month", Calendar.MONTH);
        put("day", Calendar.DATE);
        put("minute", Calendar.MINUTE);
//        put("quarter", Calendar.);
    }};
    public static final Timestamp date_trunc(String type, java.util.Date date) {
        Integer field = dateFieldType.get(type.toLowerCase());
        Asserts.notNull(field, "not supported: " + type);
        return new Timestamp(DateUtils.truncate(date, field).getTime());
    }

    // 字符串相关 -----------------------------------------------------------------------

    public static final String to_char(Object raw, String formatter) {
        if (raw instanceof Number) {
            if (StringUtils.containsAny(formatter, "y", "M", "d", "H", "h", "m", "s", "S")) {
                return DateFormatUtils.format(new java.util.Date(((Number) raw).longValue()), formatter);
            }
            DecimalFormat nf = (DecimalFormat) DecimalFormat.getInstance();
            if (formatter.contains(".")) {
                nf.setDecimalSeparatorAlwaysShown(true);
                String[] arr = formatter.split("\\.");
                formatter = arr[0].replaceAll("\\d", "#") + "." + arr[1];
            } else {
                formatter = formatter.replaceAll("\\d", "#");
                nf.setDecimalSeparatorAlwaysShown(false);
            }
            nf.applyPattern(formatter);
            return nf.format(((Number) raw).longValue());
        } else if (raw instanceof java.util.Date) {
            return DateFormatUtils.format((java.util.Date)raw, formatter);
        } else {
            throw new IllegalArgumentException("not supported: " + raw + ", " + formatter);
        }
    }

    public static final String concat(String o1, String o2) {
        return concat(o1, o2, "");
    }

    public static final String concat_ws(String seq, String o1, String o2) {
        return concat_ws(seq, o1, o2, "");
    }

    public static final String concat(String o1, String o2, String o3) {
        return concat_ws("", o1, o2, o3);
    }

    public static final String concat_ws(String seq, String o1, String o2, String o3) {
        if (o1 == null) {
            o1 = "";
        }
        if (o2 == null) {
            o2 = "";
        }
        if (o3 == null) {
            o3 = "";
        }
        return o1 + seq + o2 + seq + o3;
    }

    public static final Integer length(String str) {
        return str == null ? null : str.length();
    }
    public static final Integer octet_length(String str) {
        return str == null ? null : str.getBytes().length;
    }
    public static final String btrim(String str, String c) {
        return StringUtils.strip(str, c);
    }
    public static final String ltrim(String str, String c) {
        return StringUtils.stripStart(str, c);
    }
    public static final String rtrim(String str, String c) {
        return StringUtils.stripEnd(str, c);
    }

    public static final String left(String str, int n) {
        if (n > 0) {
            return str.substring(0, n);
        } else {
            return str.substring(0, -(n-1));
        }
    }

//    public static final String right(String str, int n) {
//        if (n > 0) {
//            return str.substring(0, n);
//        } else {
//            return str.substring(-(n-1));
//        }
//    }
    public static final String to_hex(int n) {
        return Integer.toHexString(n);
    }
    public static final String reverse(String str) {
        return StringUtils.reverse(str);
    }
    public static final String repeat(String str, int n) {
        return StringUtils.repeat(str, n);
    }
    public static final String chr(Integer c) {
        if (c == null) {
            return null;
        }
        return Character.valueOf((char)c.intValue()).toString();
    }


    public static final String md5(String raw) {
        return DigestUtils.md5Hex(raw);
    }

    public static final Number to_number(String str, String formatter) {
        String r = to_char(new BigDecimal(str.replace(",", "").replace(" ", "")), formatter.replace(",", ""));
        return new BigDecimal(r);
    }
}
