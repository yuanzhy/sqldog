package com.yuanzhy.sqldog.server.core.constant;

import com.yuanzhy.sqldog.server.core.util.DateUtil;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public enum DataType {

    INT(Integer.class), SMALLINT(Short.class), BIGINT(Long.class), NUMERIC(BigDecimal.class, true), // 数字
    SERIAL(Integer.class), SMALLSERIAL(Short.class), BIGSERIAL(Long.class), // 序列
    CHAR(String.class, true), VARCHAR(String.class, true),  // 字符串
    TEXT(String.class), BYTEA(byte[].class), // 大字段，二进制
    DATE(java.sql.Date.class), TIMESTAMP(Timestamp.class), TIME(java.sql.Time.class), // 日期时间
    BOOLEAN(Boolean.class), //
    ARRAY(Object[].class), //
    JSON(Object.class) //
    ;

//    private final int precision;
    private final Class<?> clazz;
    private final boolean hasLength;

    DataType(Class<?> clazz) {
        this(clazz, false);
    }

    DataType(Class<?> clazz, boolean hasLength) {
        this.clazz = clazz;
        this.hasLength = hasLength;
    }

    public boolean isSerial() {
        return this == SERIAL || this == SMALLSERIAL || this == BIGSERIAL;
    }

    public boolean isHasLength() {
        return hasLength;
    }

    public boolean isAssignable(Object value) {
        return clazz.isAssignableFrom(value.getClass());
    }

    public Class<?> getClazz() {
        return clazz;
    }

    public Object parseValue(String rawValue) {
        if (rawValue == null) {
            return null;
        }
        if (rawValue.startsWith("'")) {
            String defaultValue = StringUtils.substringBetween(rawValue, "'");
            if (this == DataType.DATE) {
                return DateUtil.parseSqlDate(defaultValue);
            } else if (this == DataType.TIME) {
                return DateUtil.parseSqlTime(defaultValue);
            } else if (this == DataType.TIMESTAMP) {
                return DateUtil.parseTimestamp(defaultValue);
            } else if (this == DataType.CHAR || this == DataType.VARCHAR || this == DataType.TEXT) {
                return defaultValue;
            } else {
                throw new UnsupportedOperationException(rawValue + " not supported");
            }
        } else if (rawValue.startsWith("[")) {
            throw new UnsupportedOperationException(rawValue + " not supported");
        }
//        else if (rawValue.startsWith("{") || dt == DataType.BYTEA) {
//            return rawValue;
//        }
        else if (this == DataType.BIGINT || this == DataType.BIGSERIAL) {
            return Long.valueOf(rawValue);
        } else if (this == DataType.INT || this == DataType.SERIAL) {
            return Integer.valueOf(rawValue);
        } else if (this == DataType.SMALLINT || this == DataType.SMALLSERIAL) {
            return Short.valueOf(rawValue);
        } else if (this == DataType.BOOLEAN) {
            return Boolean.valueOf(rawValue);
        } else if (this == DataType.NUMERIC) {
            return BigDecimal.valueOf(Double.parseDouble(rawValue));
        }
        return rawValue;
    }

    public static DataType of(String dataType) {
        if (dataType.contains("(")) {
            dataType = StringUtils.substringBefore(dataType, "(");
        }
        dataType = dataType.trim().toUpperCase();
        try {
            return DataType.valueOf(dataType);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Illegal data type: " + dataType, e);
        }
    }
}
