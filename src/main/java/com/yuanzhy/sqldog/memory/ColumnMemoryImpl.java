package com.yuanzhy.sqldog.memory;

import com.yuanzhy.sqldog.core.Column;
import com.yuanzhy.sqldog.core.constant.DataType;
import com.yuanzhy.sqldog.util.Asserts;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public class ColumnMemoryImpl implements Column {
    /** 名称 */
    private final String name;
    /** 数据类型 */
    private final DataType dataType;
    /** 数据长度 */
    private final int precision;
    /** 小数位数 */
    private final int scale;
    /** 空 */
    private final boolean nullable;

    ColumnMemoryImpl(String name, DataType dataType, int precision, int scale, boolean nullable) {
        this.name = name;
        this.dataType = dataType;
        this.precision = precision;
        this.scale = scale;
        this.nullable = nullable;
    }

    @Override
    public String getName() {
        return name;
    }
    @Override
    public DataType getDataType() {
        return dataType;
    }
    @Override
    public int getPrecision() {
        return precision;
    }

    @Override
    public int getScale() {
        return scale;
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

}
