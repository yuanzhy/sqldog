package com.yuanzhy.sqldog.core.constant;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public enum DataType {

    INT(32), SMALL_INT(16), BIG_INT(64), NUMERIC, // 数字
    SERIAL(32), SMALL_SERIAL(16), BIG_SERIAL(64), // 序列
    CHAR, VARCHAR,  // 字符串
    TEXT, BYTEA, // 大字段，二进制
    DATE, TIMESTAMP, TIME, // 日期时间
    BOOLEAN(1), //
    ARRAY, //
    JSON //
    ;

    private final int precision;

    DataType() {
        this.precision = 0;
    }

    DataType(int precision) {
        this.precision = precision;
    }

    public int getPrecision() {
        //if (precision == 0) {
        //    throw new IllegalArgumentException(this.name() + " must has length");
        //}
        return precision;
    }

    public boolean isSerial() {
        return this == SERIAL || this == SMALL_SERIAL || this == BIG_SERIAL;
    }
}
