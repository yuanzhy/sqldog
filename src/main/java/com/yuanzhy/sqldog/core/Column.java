package com.yuanzhy.sqldog.core;

import com.yuanzhy.sqldog.core.constant.DataType;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public interface Column extends Base {

    DataType getDataType();

    int getDataLength();

    boolean isNullable();
}
