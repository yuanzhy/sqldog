package com.yuanzhy.sqldog.memory;

import com.yuanzhy.sqldog.core.Column;
import com.yuanzhy.sqldog.core.constant.DataType;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public class ColumnMemoryImpl extends MemoryBase implements Column {
    /** 数据类型 */
    private final DataType dataType;
    /** 数据长度 */
    private final int precision;
    /** 小数位数 */
    private final int scale;
    /** 空 */
    private final boolean nullable;

    private final Object defaultValue;

    ColumnMemoryImpl(String name, DataType dataType, int precision, int scale, boolean nullable, Object defaultValue) {
        super(name.toUpperCase());
        this.dataType = dataType;
        this.precision = precision;
        this.scale = scale;
        this.nullable = nullable;
        this.defaultValue = defaultValue;
    }

    @Override
    public void drop() {

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

    @Override
    public Object defaultValue() {
        return defaultValue;
    }
}
