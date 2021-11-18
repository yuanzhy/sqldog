package com.yuanzhy.sqldog.jdbc.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.yuanzhy.sqldog.jdbc.SqldogConnection;

/**
 *
 * @author yuanzhy
 * @date 2021-11-16
 */
class StatementImpl extends AbstractStatement implements Statement {

    protected final SqldogConnection connection;
    protected final String schema;
    private final int resultSetType;
    private final int resultSetConcurrency;
    private int maxFieldSize = Short.MAX_VALUE;
    private int maxRows = Integer.MAX_VALUE;
    private int queryTimeout = 0;
    private boolean poolable = true;
    private boolean closeOnCompletion = false;


    StatementImpl(SqldogConnection connection, String schema, int resultSetType, int resultSetConcurrency) {
        this.connection = connection;
        this.schema = schema;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
    }
    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        char firstStatementChar = Util.firstAlphaCharUc(sql, Util.findStartOfStatement(sql));
        checkForDml(sql, firstStatementChar);
        return null;
    }

    protected void checkForDml(String sql, char firstStatementChar) throws SQLException {
        if ((firstStatementChar == 'I') || (firstStatementChar == 'U') || (firstStatementChar == 'D') || (firstStatementChar == 'A')
                || (firstStatementChar == 'C') || (firstStatementChar == 'T') || (firstStatementChar == 'R')) {
            String noCommentSql = Util.stripComments(sql, "'\"", "'\"", true, false, true, true);

            if (Util.startsWithIgnoreCaseAndWs(noCommentSql, "INSERT") || Util.startsWithIgnoreCaseAndWs(noCommentSql, "UPDATE")
                    || Util.startsWithIgnoreCaseAndWs(noCommentSql, "DELETE") || Util.startsWithIgnoreCaseAndWs(noCommentSql, "DROP")
                    || Util.startsWithIgnoreCaseAndWs(noCommentSql, "CREATE") || Util.startsWithIgnoreCaseAndWs(noCommentSql, "ALTER")
                    || Util.startsWithIgnoreCaseAndWs(noCommentSql, "TRUNCATE") || Util.startsWithIgnoreCaseAndWs(noCommentSql, "RENAME")) {
                throw new SQLException("Can not issue data manipulation statements with executeQuery().");
            }
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return 0;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return maxFieldSize;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        checkNum(max);
        maxFieldSize = max;
    }

    @Override
    public int getMaxRows() throws SQLException {
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        checkNum(max);
        this.maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        checkNum(seconds);
        this.queryTimeout = seconds;
    }

    @Override
    public void cancel() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {

    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return null;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkClosed();
        return resultSetConcurrency;
    }

    @Override
    public int getResultSetType() throws SQLException {
        checkClosed();
        return resultSetType;
    }

    @Override
    public void addBatch(String sql) throws SQLException {

    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkClosed();
        return this.connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        this.poolable = poolable;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return poolable;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        checkClosed();
        this.closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        checkClosed();
        return closeOnCompletion;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("does not implement '" + iface + "'");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
