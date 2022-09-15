package com.yuanzhy.sqldog.server.core.constant;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Timestamp;
import java.sql.Types;

import com.yuanzhy.sqldog.core.util.DateUtil;
import com.yuanzhy.sqldog.core.util.StringUtils;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public enum DataType {

    INT(Integer.class, Types.INTEGER, 10), SMALLINT(Short.class, Types.SMALLINT, 5), TINYINT(Byte.class, Types.TINYINT, 3), BIGINT(Long.class, Types.BIGINT, 20),
    FLOAT(Double.class, Types.FLOAT, 20), DOUBLE(Double.class, Types.DOUBLE, 20),
    NUMERIC(BigDecimal.class, Types.NUMERIC, 50, true), DECIMAL(BigDecimal.class, Types.DECIMAL, 50, true), // 数字
    SERIAL(Integer.class, Types.INTEGER, 10), SMALLSERIAL(Short.class, Types.SMALLINT, 5), BIGSERIAL(Long.class, Types.BIGINT, 20), // 序列
    CHAR(String.class, Types.CHAR, 1000, true), VARCHAR(String.class, Types.VARCHAR, 16384, true),  // 字符串
    TEXT(String.class, Types.LONGVARCHAR, Integer.MAX_VALUE), BYTEA(byte[].class, Types.BINARY, Integer.MAX_VALUE), // 大字段，二进制
    DATE(java.sql.Date.class, Types.DATE, 20), TIMESTAMP(Timestamp.class, Types.TIMESTAMP, 20), TIME(java.sql.Time.class, Types.TIME, 10), // 日期时间
    BOOLEAN(Boolean.class, Types.BOOLEAN, 1), //
    ARRAY(Array.class, Types.ARRAY, 65535), //
    JSON(Object.class, Types.JAVA_OBJECT, 65535) //
    ;

//    private final int precision;
    private final Class<?> clazz;
    private final int sqlType;
    private final int maxLength;
    private final boolean hasLength;

    DataType(Class<?> clazz, int sqlType, int maxLength) {
        this(clazz, sqlType, maxLength, false);
    }

    DataType(Class<?> clazz, int sqlType, int maxLength, boolean hasLength) {
        this.clazz = clazz;
        this.sqlType = sqlType;
        this.maxLength = maxLength;
        this.hasLength = hasLength;
    }

    public boolean isSerial() {
        return this == SERIAL || this == SMALLSERIAL || this == BIGSERIAL;
    }

    public int getMaxLength() {
        return maxLength;
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

    public int getSqlType() {
        return sqlType;
    }

    public Object parseRawValue(String rawValue) {
        if (rawValue == null) {
            return null;
        }
        if (rawValue.equals("NULL")) {
            return null;
        }
        if (rawValue.startsWith("'")) {
            String defaultValue = StringUtils.substringBetween(rawValue, "'");
            if (this == DataType.DATE) {
                return DateUtil.parseSqlDate(defaultValue).getTime();
            } else if (this == DataType.TIME) {
                return (int) DateUtil.parseSqlTime(defaultValue).getTime();
            } else if (this == DataType.TIMESTAMP) {
                return DateUtil.parse(defaultValue).getTime();
            } else if (Number.class.isAssignableFrom(this.getClazz()) || this.getClazz() == String.class) {
                return defaultValue;
            } else {
                throw new UnsupportedOperationException(rawValue + " not supported");
            }
        } else if (rawValue.startsWith("[")) {
            throw new UnsupportedOperationException(rawValue + " not supported");
        }
//        else if (rawValue.startsWith("{") || dt == DataType.BYTEA) {
//            return rawValue;
//        } // TODO 数字精度损失
        else if (this == DataType.BIGINT || this == DataType.BIGSERIAL) {
            return Long.valueOf(rawValue);
        } else if (this == DataType.INT || this == DataType.SERIAL) {
            return Integer.valueOf(rawValue);
        } else if (this == DataType.SMALLINT || this == DataType.SMALLSERIAL) {
            return Short.valueOf(rawValue);
        } else if (this == DataType.BOOLEAN) {
            return Boolean.valueOf(rawValue);
        } else if (this == DataType.NUMERIC || this == DataType.DECIMAL) {
            return new BigDecimal(rawValue);
        } else if (this == DataType.FLOAT || this == DataType.DOUBLE) {
            return Double.valueOf(rawValue);
        } else if (this.getClazz() == String.class) {
            throw new IllegalArgumentException("Illegal data type, " + this.name() + ": " + rawValue);
        }
        return rawValue;
    }

    public Object parseValue(String value) {
        if (value == null) {
            return null;
        }
        if (value.equals("NULL")) {
            return null;
        }
        if (this == DataType.DATE) {
            return DateUtil.parseSqlDate(value).getTime();
        } else if (this == DataType.TIME) {
            return (int) DateUtil.parseSqlTime(value).getTime();
        } else if (this == DataType.TIMESTAMP) {
            return DateUtil.parseTimestamp(value).getTime();
        }
//        } else if (rawValue.startsWith("[")) {
//            throw new UnsupportedOperationException(rawValue + " not supported");
//        }
//        else if (rawValue.startsWith("{") || dt == DataType.BYTEA) {
//            return rawValue;
//        } // TODO 数字精度损失
        else if (this == DataType.BIGINT || this == DataType.BIGSERIAL) {
            return Long.valueOf(value);
        } else if (this == DataType.INT || this == DataType.SERIAL) {
            return Integer.valueOf(value);
        } else if (this == DataType.SMALLINT || this == DataType.SMALLSERIAL) {
            return Short.valueOf(value);
        } else if (this == DataType.BOOLEAN) {
            return Boolean.valueOf(value);
        } else if (this == DataType.NUMERIC || this == DataType.DECIMAL) {
            return new BigDecimal(value);
        } else if (this == DataType.FLOAT || this == DataType.DOUBLE) {
            return Double.valueOf(value);
        }
        return value;
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
