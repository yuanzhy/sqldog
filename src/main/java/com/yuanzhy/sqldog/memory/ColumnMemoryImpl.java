package com.yuanzhy.sqldog.memory;

import com.yuanzhy.sqldog.core.Column;
import com.yuanzhy.sqldog.core.constant.DataType;

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
    private int dataLength;
    /** 空 */
    private final boolean nullable;

    public ColumnMemoryImpl(String name, DataType dataType) {
        this(name, dataType, dataType.getLength());
    }

    public ColumnMemoryImpl(String name, DataType dataType, boolean nullable) {
        this(name, dataType, dataType.getLength(), nullable);
    }

    public ColumnMemoryImpl(String name, DataType dataType, int dataLength) {
        this(name, dataType, dataLength, true);
    }

    public ColumnMemoryImpl(String name, DataType dataType, int dataLength, boolean nullable) {
        this.name = name;
        this.dataType = dataType;
        this.dataLength = dataLength;
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
    public int getDataLength() {
        return dataLength;
    }
    @Override
    public boolean isNullable() {
        return nullable;
    }
}
