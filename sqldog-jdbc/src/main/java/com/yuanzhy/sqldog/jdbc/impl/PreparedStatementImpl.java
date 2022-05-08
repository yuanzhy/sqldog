package com.yuanzhy.sqldog.jdbc.impl;

import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.core.util.SqlUtil;
import com.yuanzhy.sqldog.jdbc.SqldogConnection;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.UUID;

/**
 *
 * @author yuanzhy
 * @date 2021-11-16
 */
class PreparedStatementImpl extends StatementImpl implements PreparedStatement {

    private final int autoGenerateKey;
    final String preparedSql;
    final String preparedId;
    private final ResultSetMetaData rsmd;
    private final ParameterMetaData pmd;
    private final int parameterCount;
//    private final char firstStatementChar;
    final Object[] parameter;
    private final boolean isDml;
    private final List<Object[]> parameterList = new ArrayList<>();
    PreparedStatementImpl(SqldogConnection connection, String schema, String preparedSql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        this(connection, schema, preparedSql, resultSetType, resultSetConcurrency, RETURN_GENERATED_KEYS);
    }

    PreparedStatementImpl(SqldogConnection connection, String schema, String preparedSql, int resultSetType,
                          int resultSetConcurrency, int autoGenerateKey) throws SQLException {
        super(connection, schema, resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        checkNullOrEmpty(preparedSql);
        this.preparedSql = preparedSql;
        this.preparedId = UUID.randomUUID().toString();
        this.autoGenerateKey = autoGenerateKey;
        String noCommentSql = SqlUtil.stripComments(preparedSql, "'\"", "'\"", true, false, true, true).trim();
        if (Util.startsWithIgnoreCaseAndWs(noCommentSql, "INSERT ")
                || Util.startsWithIgnoreCaseAndWs(noCommentSql, "UPDATE ")
                || Util.startsWithIgnoreCaseAndWs(noCommentSql, "DELETE ")) {
            isDml = true;
        } else if (Util.startsWithIgnoreCaseAndWs(noCommentSql, "SELECT ")
//                || Util.startsWithIgnoreCaseAndWs(noCommentSql, "WITH RECURSIVE ")
        ) {
            isDml = false;
        } else {
            throw new SQLException("Not supported PreparedStatement: " + preparedSql);
        }
//        this.firstStatementChar = Util.firstAlphaCharUc(preparedSql, Util.findStartOfStatement(preparedSql));
        SqlResult result = this.connection.prepareExecute(this, preparedId, preparedSql);
        this.rsmd = new ResultSetMetaDataImpl(result.getColumns());
        this.pmd = new ParameterMetaDataImpl(result.getParams());
        this.parameterCount = this.pmd.getParameterCount();
        parameter = new Object[this.parameterCount];
    }

    private void executeInternal() throws SQLException {
        beforeExecute();
        try {
            SqlResult sqlResult = connection.executePrepared(this, 0, preparedId, preparedSql, parameter);
            this.handleResult(sqlResult);
        } finally {
            afterExecute();
        }
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkClosed();
        if (isDml) {
            throw new SQLException("Can not issue data manipulation statements with executeQuery().");
        }
        this.executeInternal();
        return rs;
    }

    @Override
    public int executeUpdate() throws SQLException {
        return (int)executeLargeUpdate();
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        checkClosed();
        this.executeInternal();
        return rows;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        this.setObject(parameterIndex, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        this.setObject(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        this.setObject(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        this.setObject(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        this.setObject(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        this.setObject(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        this.setObject(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        this.setObject(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        this.setObject(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        this.setObject(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        this.setObject(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        this.setObject(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        this.setObject(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        this.setObject(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            this.setBytes(parameterIndex, Util.toByteArray(x, length));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void clearParameters() throws SQLException {
        checkClosed();
        this.parameterList.clear();
        for (int i = 0; i < this.parameter.length; i++) {
            this.parameter[i] = null;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, targetSqlType);
        }
        switch (targetSqlType) {
            case Types.CLOB:
            case Types.DATALINK:
            case Types.NCLOB:
            case Types.OTHER:
            case Types.REF:
            case Types.SQLXML:
            case Types.STRUCT:
                throw SqlUtil.notImplemented();
            case Types.ARRAY:
                setArray(parameterIndex, SqlUtil.toArray(x));
                break;
            case Types.BIGINT:
                setLong(parameterIndex, SqlUtil.toLong(x));
                break;
            case Types.BINARY:
            case Types.LONGVARBINARY:
            case Types.VARBINARY:
                setBytes(parameterIndex, SqlUtil.toBytes(x));
                break;
            case Types.BIT:
            case Types.BOOLEAN:
                setBoolean(parameterIndex, SqlUtil.toBoolean(x));
                break;
            case Types.BLOB:
                if (x instanceof Blob) {
                    setBlob(parameterIndex, (Blob) x);
                    break;
                } else if (x instanceof InputStream) {
                    setBlob(parameterIndex, (InputStream) x);
                }
                throw SqlUtil.unsupportedCast(x.getClass(), Blob.class);
            case Types.DATE:
                setDate(parameterIndex, SqlUtil.toDate(x));
                break;
            case Types.DECIMAL:
            case Types.NUMERIC:
                setBigDecimal(parameterIndex, SqlUtil.toBigDecimal(x));
                break;
            case Types.DISTINCT:
                throw SqlUtil.notImplemented();
            case Types.DOUBLE:
            case Types.FLOAT: // yes really; SQL FLOAT is up to 8 bytes
                setDouble(parameterIndex, SqlUtil.toDouble(x));
                break;
            case Types.INTEGER:
                setInt(parameterIndex, SqlUtil.toInt(x));
                break;
            case Types.JAVA_OBJECT:
                setObject(parameterIndex, x);
                break;
            case Types.LONGNVARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.NCHAR:
                String v = SqlUtil.toString(x);
                Asserts.isTrue(v.length() <= pmd.getPrecision(parameterIndex), "Exceeding maximum limit length: " + v.length());
                setString(parameterIndex, v);
                break;
            case Types.REAL:
                setFloat(parameterIndex, SqlUtil.toFloat(x));
                break;
            case Types.ROWID:
                if (x instanceof RowId) {
                    setRowId(parameterIndex, (RowId) x);
                    break;
                }
                throw SqlUtil.unsupportedCast(x.getClass(), RowId.class);
            case Types.SMALLINT:
                setShort(parameterIndex, SqlUtil.toShort(x));
                break;
            case Types.TIME:
                setTime(parameterIndex, SqlUtil.toTime(x));
                break;
            case Types.TIMESTAMP:
                setTimestamp(parameterIndex, SqlUtil.toTimestamp(x));
                break;
            case Types.TINYINT:
                setByte(parameterIndex, SqlUtil.toByte(x));
                break;
            default:
                throw SqlUtil.notImplemented();
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        checkParamBounds(parameterIndex);
        this.parameter[parameterIndex - 1] = x;
    }

    private void checkParamBounds(int parameterIndex) throws SQLException {
        checkClosed();
        if (parameterIndex < 1) {
            throw new SQLException("Column Index out of range, " + parameterIndex + " < 1.");
        } else if (parameterIndex > parameterCount) {
            throw new SQLException("Column Index out of range, " + parameterIndex + " > " + parameterCount + ".");
        }
    }

    @Override
    public boolean execute() throws SQLException {
        checkClosed();
        this.executeInternal();
        return this.rs != null;
    }

    @Override
    public void addBatch() throws SQLException {
        checkClosed();
        parameterList.add(parameter.clone());
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        checkClosed();
        SqlResult[] results = this.connection.executePrepared(this, 0, preparedId, preparedSql, parameterList);
        long[] r = new long[results.length];
        for (int i = 0; i < results.length; i++) {
            r[i] = results[i].getRows();
        }
        return r;
    }

    @Override
    public void clearBatch() throws SQLException {
        this.clearParameters();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return rsmd;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        checkParamBounds(parameterIndex);
        // noop
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        this.setString(parameterIndex, x.toString());
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        checkClosed();
        return pmd;
    }

    @Override
    public void close() throws SQLException {
        if (isClosed()) {
            return;
        }
        try {
            this.clearParameters();
        } catch (SQLException e) {
            // ignore
        }
        super.close();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
            throws SQLException {
        // TODO 未实现 scaleOrLength
        this.setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        try {
            this.setBytes(parameterIndex, Util.toByteArray(x));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
