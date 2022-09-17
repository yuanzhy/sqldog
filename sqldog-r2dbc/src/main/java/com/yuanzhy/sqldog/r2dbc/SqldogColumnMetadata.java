package com.yuanzhy.sqldog.r2dbc;

import java.sql.DatabaseMetaData;

import com.yuanzhy.sqldog.core.sql.ColumnMetaData;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.Type;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/9/12
 */
class SqldogColumnMetadata implements ColumnMetadata {

    private final ColumnMetaData columnMetaData;
    private final Type type;
    private Class<?> cls;
    SqldogColumnMetadata(ColumnMetaData columnMetaData) {
        this.columnMetaData = columnMetaData;
        this.type = new Type() {
            @Override
            public Class<?> getJavaType() {
                if (cls == null) {
                    try {
                        cls = Class.forName(columnMetaData.getColumnClassName());
                    } catch (ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                }
                return cls;
            }
            @Override
            public String getName() {
                return columnMetaData.getColumnName();
            }
        };
    }

    public int getIndex() {
        return this.columnMetaData.getOrdinal() - 1;
    }

    @Override
    public Class<?> getJavaType() {
        return type.getJavaType();
    }

    @Override
    public Type getType() {
        return type;
    }

    @Override
    public String getName() {
        return columnMetaData.getColumnName();
    }

    @Override
    public Object getNativeTypeMetadata() {
        return ColumnMetadata.super.getNativeTypeMetadata();
    }

    @Override
    public Nullability getNullability() {
        switch (columnMetaData.getNullable()) {
            case DatabaseMetaData.columnNullable:
                return Nullability.NULLABLE;
            case DatabaseMetaData.columnNoNulls:
                return Nullability.NON_NULL;
            default:
                return Nullability.UNKNOWN;
        }
    }

    @Override
    public Integer getPrecision() {
        return columnMetaData.getPrecision();
    }

    @Override
    public Integer getScale() {
        return columnMetaData.getScale();
    }
}
