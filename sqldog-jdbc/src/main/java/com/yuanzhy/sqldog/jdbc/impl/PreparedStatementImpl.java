package com.yuanzhy.sqldog.jdbc.impl;

import com.yuanzhy.sqldog.core.util.SqlUtil;
import com.yuanzhy.sqldog.jdbc.SqldogConnection;
import org.apache.commons.io.IOUtils;

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
import java.util.Calendar;

/**
 *
 * @author yuanzhy
 * @date 2021-11-16
 */
class PreparedStatementImpl extends StatementImpl implements PreparedStatement {

    private final int autoGenerateKey;
    private final String preparedSql;
//    private final char firstStatementChar;
    private final Object[] parameters;
    private final boolean isDml;
    PreparedStatementImpl(SqldogConnection connection, String schema, String preparedSql, int resultSetType,
            int resultSetConcurrency) throws SQLException {
        this(connection, schema, preparedSql, resultSetType, resultSetConcurrency, RETURN_GENERATED_KEYS);
    }

    PreparedStatementImpl(SqldogConnection connection, String schema, String preparedSql, int resultSetType,
                          int resultSetConcurrency, int autoGenerateKey) throws SQLException {
        super(connection, schema, resultSetType, resultSetConcurrency);
        checkNullOrEmpty(preparedSql);
        this.preparedSql = preparedSql;
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
        this.parameters = new Object[SqlUtil.countQuestionMark(preparedSql)];
//        this.connection.
        // TODO
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkClosed();
        if (isDml) {
            throw new SQLException("Can not issue data manipulation statements with executeQuery().");
        }
// TODO
        return null;
    }

    @Override
    public int executeUpdate() throws SQLException {
        // TODO
        return 0;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkParamBounds(parameterIndex);
        // noop
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
            this.setBytes(parameterIndex, IOUtils.toByteArray(x, length));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void clearParameters() throws SQLException {
        checkClosed();
        for (int i = 0; i < this.parameters.length; i++) {
            this.parameters[i] = null;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
// TODO
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        checkParamBounds(parameterIndex);
        this.parameters[parameterIndex - 1] = x;
    }

    private void checkParamBounds(int parameterIndex) throws SQLException {
        checkClosed();
        if (parameterIndex < 1) {
            throw new SQLException("Column Index out of range, " + parameterIndex + " < 1.");
        } else if (parameterIndex > parameters.length) {
            throw new SQLException("Column Index out of range, " + parameterIndex + " > " + parameters.length + ".");
        }
    }

    @Override
    public boolean execute() throws SQLException {// TODO
        return false;
    }

    @Override
    public void addBatch() throws SQLException {
// TODO
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
        // TODO
        return null;
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
        // TODO
        return null;
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
        // TODO
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
            this.setBytes(parameterIndex, IOUtils.toByteArray(x));
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
