package com.yuanzhy.sqldog.server.memory;

import com.yuanzhy.sqldog.server.core.builder.BaseBuilder;
import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.core.constant.DataType;
import com.yuanzhy.sqldog.server.core.util.Asserts;

/**
 *
 * @author yuanzhy
 * @date 2021-10-26
 */
public class ColumnBuilder extends BaseBuilder<ColumnBuilder> {
    private DataType dataType;
    private int precision;
    private int scale;
    private boolean nullable = true;
    private Object defaultValue;

    @Override
    protected ColumnBuilder getSelf() {
        return this;
    }

    public ColumnBuilder dataType(DataType dataType) {
        this.dataType = dataType;
        return this;
    }

    public ColumnBuilder precision(int precision) {
        this.precision = precision;
        return this;
    }

    public ColumnBuilder scale(int scale) {
        this.scale = scale;
        return this;
    }

    public ColumnBuilder nullable(boolean nullable) {
        this.nullable = nullable;
        return this;
    }

    public ColumnBuilder defaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    @Override
    public Column build() {
        Asserts.hasText(name, "列名不能为空");
        Asserts.notNull(dataType, "数据类型不能为空");
        Asserts.isFalse(dataType.isHasLength() && this.precision == 0, "数据长度不能为空");
        return new ColumnMemoryImpl(name, dataType, precision, scale, nullable, defaultValue);
    }
}