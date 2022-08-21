package com.yuanzhy.sqldog.server.storage.builder;

import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.core.util.StringUtils;
import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.core.constant.DataType;
import com.yuanzhy.sqldog.server.storage.memory.MemoryColumn;

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
        if (dataType.isHasLength()) {
            Asserts.isTrue(this.precision > 0, name + " 数据长度不能为空");
            Asserts.isTrue(this.precision <= dataType.getMaxLength(), name + " 数据长度超过限制：" + dataType.getMaxLength());
        }
        if (defaultValue != null && dataType == DataType.CHAR) {
            defaultValue = StringUtils.rightPad(defaultValue.toString(), this.precision);
        }
        return new MemoryColumn(name, dataType, precision, scale, nullable, defaultValue);
    }
}