package com.yuanzhy.sqldog.core;

import com.yuanzhy.sqldog.core.constant.DataType;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public interface Column extends Base {

    DataType getDataType();

    /**
     * 数据精度 int, numeric等
     * @return
     */
    int getPrecision();

    /**
     * 小数位数 numeric
     * @return
     */
    int getScale();

    /**
     * 长度, 字符串用
     * @return
     */
    int getLength();

    boolean isNullable();
}
