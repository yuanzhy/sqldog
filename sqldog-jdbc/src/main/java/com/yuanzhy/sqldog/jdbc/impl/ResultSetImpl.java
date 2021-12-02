package com.yuanzhy.sqldog.jdbc.impl;

import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.core.util.DateUtil;
import com.yuanzhy.sqldog.jdbc.SQLError;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 *
 * @author yuanzhy
 * @date 2021-11-16
 */
class ResultSetImpl extends AbstractResultSet implements ResultSet {

    protected final AbstractStatement statement;
    protected final SqlResult sqlResult;
    protected final List<Object[]> data;
    protected final ResultSetMetaData metaData;
    private boolean closed = false;
    private int fetchSize = 0; // TODO cursor need it
    private int direction; // TODO not support
    private boolean wasNull = false;
    /**
     * The above map is a case-insensitive tree-map, it can be slow, this caches lookups into that map, because the other alternative is to create new
     * object instances for every call to findColumn()....
     */
    protected Map<String, Integer> columnToIndexCache = new HashMap<>();
    private int rowIndex = -1;

    ResultSetImpl(AbstractStatement statement, int direction, int fetchSize, SqlResult sqlResult) {
        this.statement = statement;
        this.direction = direction;
        this.fetchSize = fetchSize;
        this.sqlResult = sqlResult;
        this.data = sqlResult.getData() == null ? Collections.emptyList() : sqlResult.getData();
        this.metaData = new ResultSetMetaDataImpl(sqlResult.getColumns());
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        if (isAfterLast()) {
            return false;
        }
        rowIndex++;
        if (data.size() <= rowIndex) {
            return false;
        }
        return true;
    }

    @Override
    public void close() throws SQLException {
        if (this.closed) {
            return;
        }
        this.closed = true;
        this.columnToIndexCache.clear();
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return getObject(columnIndex, String.class);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return getObject(columnIndex, Boolean.class);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return getObject(columnIndex, Byte.class);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return getObject(columnIndex, Short.class);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return getObject(columnIndex, Integer.class);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return getObject(columnIndex, Long.class);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return getObject(columnIndex, Float.class);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return getObject(columnIndex, Double.class);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        BigDecimal bigDecimal = getObject(columnIndex, BigDecimal.class);
        return bigDecimal.setScale(scale);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return getObject(columnIndex, byte[].class);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return getObject(columnIndex, Date.class);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return getObject(columnIndex, Time.class);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getObject(columnIndex, Timestamp.class);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return getObject(columnIndex, InputStream.class);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return this.metaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkRowPos();
        checkColumnBounds(columnIndex);
        Object value = row()[columnIndex - 1];
        wasNull = value == null;
        return value;
//        String className = this.metaData.getColumnClassName(columnIndex);
//        if (value.getClass().getName().equals(className)) {
//            return value;
//        }
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkRowPos();
        columnLabel = columnLabel.toUpperCase();
        Integer index = columnToIndexCache.get(columnLabel);
        if (index != null) {
            return index.intValue();
        }
        int columnIndex = 0;
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            if (metaData.getColumnLabel(i).equals(columnLabel)) {
                columnIndex = i;
                break;
            }
        }
        if (columnIndex == 0) {
            throw new SQLException("Column '" + columnLabel + "' not found.");
        }
        columnToIndexCache.put(columnLabel, columnIndex);
        return columnIndex;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return getObject(columnIndex, BigDecimal.class);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkClosed();
        return rowIndex < 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkClosed();
        return this.data.size() <= rowIndex;
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkClosed();
        return rowIndex == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkClosed();
        return rowIndex - 1 == this.data.size();
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkScrollType();
        this.rowIndex = -1;
    }

    @Override
    public void afterLast() throws SQLException {
        checkScrollType();
        this.rowIndex = data.size();
    }

    @Override
    public boolean first() throws SQLException {
        checkScrollType();
        this.rowIndex = 0;
        return !data.isEmpty();
    }

    @Override
    public boolean last() throws SQLException {
        checkScrollType();
        this.rowIndex = data.size() - 1;
        return this.rowIndex >= 0;
    }

    @Override
    public int getRow() throws SQLException {
        checkClosed();
        return this.rowIndex + 1;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        checkScrollType();
        checkNum(row);
        if (row > data.size()) {
            return false;
        }
        this.rowIndex = row - 1;
        return true;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        checkScrollType();
        int rowIndex = this.rowIndex + rows;
        if (rowIndex < 0 || this.data.size() <= rowIndex) {
            return false;
        }
        this.rowIndex = rowIndex;
        return true;
    }

    @Override
    public boolean previous() throws SQLException {
        checkScrollType();
        if (rowIndex == 0) {
            return false;
        }
        this.rowIndex--;
        return true;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkClosed();
        if ((direction != FETCH_FORWARD) && (direction != FETCH_REVERSE) && (direction != FETCH_UNKNOWN)) {
            throw new SQLException("Illegal value for fetch direction", SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
        }
        this.direction = direction;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return direction;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return this.fetchSize;
    }

    @Override
    public int getType() throws SQLException {
        return statement.getResultSetType();
    }

    @Override
    public int getConcurrency() throws SQLException {
        return statement.getResultSetConcurrency();
    }

    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndex);
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return getObject(columnIndex, Blob.class);
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return getObject(columnIndex, Clob.class);
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return getObject(columnIndex, Array.class);
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(columnLabel), map);
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(findColumn(columnLabel));
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return getClob(findColumn(columnLabel));
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return getArray(findColumn(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        Date date = getDate(columnIndex);
        if (cal == null) {
            return date;
        }
        // TODO
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel), cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        // TODO
        return null;
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        // TODO
        return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel), cal);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        try {
            return new URL(getString(columnIndex));
        } catch (MalformedURLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getURL(findColumn(columnLabel));
    }

    @Override
    public int getHoldability() throws SQLException {
        return statement.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        if (type == null) {
            throw new SQLException("Type parameter can not be null");
        }
        Object value = getObject(columnIndex);
        if (value == null) {
            return null;
        }
        if (type.isInstance(value)) {
            if (value instanceof Cloneable) {
                return (T)((java.util.Date)value).clone();
            }
            return (T) value;
        }
        if (type.equals(String.class)) {
            return (T) Objects.toString(value);
        } else if (type.equals(BigDecimal.class)) {
            return (T) new BigDecimal(Objects.toString(value));
        } else if (type.equals(Boolean.class) || type.equals(Boolean.TYPE)) {
            return (T) Boolean.valueOf(Objects.toString(value));
        } else if (type.equals(Integer.class) || type.equals(Integer.TYPE)) {
            return (T) Integer.valueOf(Objects.toString(value));
        } else if (type.equals(Long.class) || type.equals(Long.TYPE)) {
            return (T) Long.valueOf(Objects.toString(value));
        } else if (type.equals(Float.class) || type.equals(Float.TYPE)) {
            return (T) Float.valueOf(Objects.toString(value));
        } else if (type.equals(Double.class) || type.equals(Double.TYPE)) {
            return (T) Double.valueOf(Objects.toString(value));
        } else if (type.equals(Short.class) || type.equals(Short.TYPE)) {
            return (T) Short.valueOf(Objects.toString(value));
        } else if (type.equals(byte[].class)) {
            return (T) Objects.toString(value).getBytes();
        } else if (type.equals(java.sql.Date.class)) {
            return (T) DateUtil.parseSqlDate(Objects.toString(value));
        } else if (type.equals(Time.class)) {
            return (T) DateUtil.parseSqlTime(Objects.toString(value));
        } else if (type.equals(Timestamp.class)) {
            return (T) DateUtil.parseTimestamp(Objects.toString(value));
        } else if (type.equals(Clob.class)) {
            throw new SQLFeatureNotSupportedException();
        } else if (type.equals(Blob.class)) {
            throw new SQLFeatureNotSupportedException();
        } else if (type.equals(Array.class)) {
            throw new SQLFeatureNotSupportedException(); // TODO
        } else if (type.equals(Ref.class)) {
            throw new SQLFeatureNotSupportedException();
        } else if (type.equals(URL.class)) {
            return (T) getURL(columnIndex);
        } else if (type.equals(InputStream.class)) {
            if (value instanceof byte[]) {
                return (T) new ByteArrayInputStream((byte[])value);
            }
            return (T) new ByteArrayInputStream(Objects.toString(value).getBytes());
        } else {
            try {
                return type.cast(value);
            } catch (ClassCastException cce) {
                SQLException sqlEx = new SQLException("Conversion not supported for type " + type.getName(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
                sqlEx.initCause(cce);
                throw sqlEx;
            }
        }
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
    }

    protected void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("No operations allowed after resultSet closed.");
        }
        if (this.statement != null) {
            this.statement.checkClosed();
        }
    }

    protected void checkRowPos() throws SQLException {
        checkClosed();
        if (this.data.isEmpty()) {
            throw new SQLException("Illegal operation on empty result set.");
        }
        if (isBeforeFirst()) {
            throw new SQLException("Before start of result set");
        }
        if (isAfterLast()) {
            throw new SQLException("After end of result set");
        }
    }

    protected Object[] row() {
        return data.get(rowIndex);
    }

    protected final void checkColumnBounds(int columnIndex) throws SQLException {
        if (columnIndex < 1) {
            throw new SQLException("Column Index out of range, " + columnIndex + " < 1.");
        } else if (columnIndex > row().length) {
            throw new SQLException("Column Index out of range, " + columnIndex + " > " + row().length + ".");
        }
    }

    protected void checkScrollType() throws SQLException {
        checkClosed();
        if (statement.getResultSetType() == ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLException("Operation not supported for streaming result sets");
        }
    }

    protected void checkNum(int num) throws SQLException {
        if (num <= 1) {
            throw new SQLException("number must great than 1");
        }
    }
}
