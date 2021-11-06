package com.yuanzhy.sqldog.server.core;

import com.yuanzhy.sqldog.server.core.constant.DataType;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public interface Column extends Base {
    /**
     * 数据类型
     * @return
     */
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
     * 可为空
     * @return
     */
    boolean isNullable();

    /**
     * 默认值
     * @return
     */
    Object defaultValue();
}
