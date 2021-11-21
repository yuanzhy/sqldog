package com.yuanzhy.sqldog.jdbc.impl;

import com.yuanzhy.sqldog.core.sql.ColumnMetaData;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/20
 */
class ResultSetMetaDataImpl extends AbstractWrapper implements ResultSetMetaData {

    private final ColumnMetaData[] columns;

    ResultSetMetaDataImpl(ColumnMetaData[] columns) {
        this.columns = columns;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columns.length;
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return getColumnMetaData(column).isAutoIncrement();
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return getColumnMetaData(column).isCaseSensitive();
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return getColumnMetaData(column).isSearchable();
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return getColumnMetaData(column).isCurrency();
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return getColumnMetaData(column).getNullable();
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return getColumnMetaData(column).isSigned();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return getColumnMetaData(column).getDisplaySize();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnMetaData(column).getLabel();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return getColumnMetaData(column).getColumnName();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return getColumnMetaData(column).getSchemaName();
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return getColumnMetaData(column).getPrecision();
    }

    @Override
    public int getScale(int column) throws SQLException {
        return getColumnMetaData(column).getScale();
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return getColumnMetaData(column).getTableName();
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return getColumnMetaData(column).getCatalogName();
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return getColumnMetaData(column).getColumnType();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return getColumnMetaData(column).getColumnTypeName();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return getColumnMetaData(column).isReadOnly();
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return getColumnMetaData(column).isWritable();
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return getColumnMetaData(column).isDefinitelyWritable();
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return getColumnMetaData(column).getColumnClassName();
    }

    private ColumnMetaData getColumnMetaData(int column) throws SQLException {
        if (column < 1) {
            throw new SQLException("Column Index out of range, " + column + " < 1.");
        } else if (column > columns.length) {
            throw new SQLException("Column Index out of range, " + column + " > " + columns.length + ".");
        }
        return columns[column - 1];
    }
}
