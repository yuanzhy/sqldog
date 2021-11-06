package com.yuanzhy.sqldog.server.core.constant;

import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public enum DataType {

    INT(Integer.class), SMALLINT(Short.class), BIGINT(Long.class), NUMERIC(Number.class, true), // 数字
    SERIAL(Integer.class), SMALLSERIAL(Short.class), BIGSERIAL(Long.class), // 序列
    CHAR(String.class, true), VARCHAR(String.class, true),  // 字符串
    TEXT(String.class), BYTEA(Object.class), // 大字段，二进制
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

    public static DataType of(String dataType) {
        if (dataType.contains("(")) {
            dataType = StringUtils.substringBefore(dataType, "(");
        }
        dataType = dataType.trim().toUpperCase();
        return DataType.valueOf(dataType);
    }
}
