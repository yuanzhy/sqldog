package com.yuanzhy.sqldog.core.sql;

import java.io.Serializable;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/20
 */
public interface ColumnMetaData extends Serializable {

    int getOrdinal();

    boolean isAutoIncrement();

    boolean isCaseSensitive();

    boolean isSearchable();

    boolean isCurrency();

    int getNullable();

    boolean isSigned();

    int getDisplaySize();

    String getLabel();

    String getColumnName();

    String getSchemaName();

    int getPrecision();

    int getScale();

    String getTableName();

    String getCatalogName();

    boolean isReadOnly();

    boolean isWritable();

    boolean isDefinitelyWritable();

    String getColumnClassName();

    int getColumnType();
    String getColumnTypeName();
}
