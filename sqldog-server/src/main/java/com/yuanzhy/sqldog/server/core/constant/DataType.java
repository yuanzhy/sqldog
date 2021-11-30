package com.yuanzhy.sqldog.server.core.constant;

import com.yuanzhy.sqldog.core.util.DateUtil;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public enum DataType {

    INT(Integer.class, Types.INTEGER), SMALLINT(Short.class, Types.SMALLINT), TINYINT(Byte.class, Types.TINYINT), BIGINT(Long.class, Types.BIGINT),
    NUMERIC(BigDecimal.class, Types.NUMERIC, true), DECIMAL(BigDecimal.class, Types.DECIMAL, true), // 数字
    SERIAL(Integer.class, Types.INTEGER), SMALLSERIAL(Short.class, Types.SMALLINT), BIGSERIAL(Long.class, Types.BIGINT), // 序列
    CHAR(String.class, Types.CHAR, true), VARCHAR(String.class, Types.VARCHAR, true),  // 字符串
    TEXT(String.class, Types.LONGVARCHAR), BYTEA(byte[].class, Types.BINARY), // 大字段，二进制
    DATE(Long.class, Types.DATE), TIMESTAMP(Long.class, Types.TIMESTAMP), TIME(java.sql.Time.class, Types.TIME), // 日期时间
    BOOLEAN(Boolean.class, Types.BOOLEAN), //
    ARRAY(Array.class, Types.ARRAY), //
    JSON(Object.class, Types.JAVA_OBJECT) //
    ;

//    private final int precision;
    private final Class<?> clazz;
    private final int sqlType;
    private final boolean hasLength;

    DataType(Class<?> clazz, int sqlType) {
        this(clazz, sqlType, false);
    }

    DataType(Class<?> clazz, int sqlType, boolean hasLength) {
        this.clazz = clazz;
        this.sqlType = sqlType;
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
                return DateUtil.parseSqlTime(defaultValue);
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
        } else if (this == DataType.NUMERIC) {
            return BigDecimal.valueOf(Double.parseDouble(rawValue));
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
            return DateUtil.parseSqlTime(value);
        } else if (this == DataType.TIMESTAMP) {
            return DateUtil.parse(value).getTime();
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
        } else if (this == DataType.NUMERIC) {
            return BigDecimal.valueOf(Double.parseDouble(value));
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
