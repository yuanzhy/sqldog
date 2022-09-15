package com.yuanzhy.sqldog.jdbc.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.core.util.SqlUtil;
import com.yuanzhy.sqldog.jdbc.SQLError;
import com.yuanzhy.sqldog.jdbc.SqldogConnection;

/**
 *
 * @author yuanzhy
 * @date 2021-11-16
 */
class StatementImpl extends AbstractStatement implements Statement {

    protected final String schema;
    private final int resultSetType;
    private final int resultSetConcurrency;
    private final int resultSetHoldability;
    private final Set<ResultSet> openResultSets = new HashSet<>();
    private final List<String> sqlList = new ArrayList<>();
    private int maxFieldSize = Integer.MAX_VALUE;
    private int maxRows = Integer.MAX_VALUE;
    private int queryTimeout = 60; // TODO 暂不支持cancel, 先放个1分钟超时
    private int direction = ResultSet.FETCH_FORWARD;
    private int fetchSize = 200;
    private boolean escapeProcessing = false;
    private boolean poolable = true;
    private boolean closeOnCompletion = false;
    protected volatile ResultSetImpl rs;
    protected volatile long rows = -1;

    String sql;

    StatementImpl(SqldogConnection connection, String schema, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        super(connection);
        this.schema = schema;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;
    }
    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();
        checkNullOrEmpty(sql);
        char firstStatementChar = Util.firstAlphaCharUc(sql, Util.findStartOfStatement(sql));
        checkForDml(sql, firstStatementChar);
        executeInternal(sql);
        return rs;
    }

    protected void executeInternal(String sql) throws SQLException {
        beforeExecute();
        try {
            this.sql = sql;
            if (sql.contains(";")) {
                String[] arr = sql.split("(;\\s*\n?)");
                if (arr.length == 1) {
                    SqlResult sqlResult = connection.execute(this, sql);
                    this.handleResult(sqlResult);
                } else {
                    SqlResult[] sqlResults = connection.execute(this, 0, arr);
                    this.handleResult(sqlResults);
                }
            } else {
                SqlResult sqlResult = connection.execute(this, sql);
                this.handleResult(sqlResult);
            }
        } finally {
            afterExecute();
        }
    }

    protected void handleResult(SqlResult... results) {
        try {
            closeAllResultSets();
        } catch (SQLException e) {
            // ignore
        }
        SqlResult result = results[results.length - 1];
        this.rows = result.getRows();
        if (result.getData() == null /*|| result.getData().isEmpty()*/) {
            this.rs = null;
//            this.rows = result.getRows();
            if (result.getType() == StatementType.SWITCH_SCHEMA) {
                ((ConnectionImpl) this.connection).schema = result.getSchema();
                return;
            }
        } else {
            ResultSetImpl rs = resultSetConcurrency == ResultSet.CONCUR_READ_ONLY ?
                    new ResultSetImpl(this, direction, fetchSize, result) :
                    new UpdatedResultSetImpl(this, direction, 0, result);
            this.openResultSets.add(rs);
            this.rs = rs;
        }
        if (results.length > 1) {
            for (int i = results.length - 2; i >= 0; i--) {
                if (results[i].getType() == StatementType.SWITCH_SCHEMA) {
                    ((ConnectionImpl) this.connection).schema = results[i].getSchema();
                    return;
                }
            }
        }
    }

    protected void checkForDml(String sql, char firstStatementChar) throws SQLException {
        if ((firstStatementChar == 'I') || (firstStatementChar == 'U') || (firstStatementChar == 'D') || (firstStatementChar == 'A')
                || (firstStatementChar == 'C') || (firstStatementChar == 'T') || (firstStatementChar == 'R')) {
            String noCommentSql = SqlUtil.stripComments(sql, "'\"", "'\"", true, false, true, true);

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
        return (int)executeLargeUpdate(sql);
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        return this.executeLargeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        checkClosed();
        return maxFieldSize;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        checkClosed();
        checkNum(max);
        maxFieldSize = max;
    }

    @Override
    public int getMaxRows() throws SQLException {
        checkClosed();
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        checkClosed();
        checkNum(max);
        this.maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        checkClosed();
        this.escapeProcessing = enable;
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        checkClosed();
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        checkClosed();
        checkNum(seconds);
        this.queryTimeout = seconds;
    }

    @Override
    public void close() throws SQLException {
        if (this.isClosed()) {
            return;
        }
        this.closed = true;
        this.sqlList.clear();
        this.clearWarnings();
        this.closeAllResultSets();
        this.connection.close(this);
    }

    protected void closeAllResultSets() throws SQLException {
        SQLException sqlEx = null;
        for (ResultSet rs : openResultSets) {
            try {
                rs.close();
            } catch (SQLException e) {
                sqlEx = e;
            }
        }
        openResultSets.clear();
        if (sqlEx != null) {
            throw sqlEx;
        }
    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return execute(sql, Statement.RETURN_GENERATED_KEYS);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();
        return rs;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return (int)getLargeUpdateCount();
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        checkClosed();
        return rows;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return getMoreResults(Statement.CLOSE_CURRENT_RESULT);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkClosed();
        if ((direction != ResultSet.FETCH_FORWARD) && (direction != ResultSet.FETCH_REVERSE) && (direction != ResultSet.FETCH_UNKNOWN)) {
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
        checkNum(rows);
        if (this.resultSetConcurrency == ResultSet.CONCUR_READ_ONLY
                && this.direction == ResultSet.FETCH_FORWARD
                && this.resultSetType == ResultSet.TYPE_FORWARD_ONLY) {
            this.fetchSize = rows;
        }
        // rs可编辑模式或者逆序模式 不支持fetchSize, 只能一次性获取服务器数据
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return fetchSize;
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
        checkClosed();
        this.sqlList.add(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        checkClosed();
        this.sqlList.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkClosed();
        long[] longArr = executeLargeBatch();
        int[] r = new int[longArr.length];
        for (int i = 0; i < longArr.length; i++) {
            r[i] = (int)longArr[i];
        }
        return r;
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        checkClosed();
        SqlResult[] results = this.connection.execute(this, 0, sqlList.toArray(new String[0]));
        long[] r = new long[results.length];
        for (int i = 0; i < results.length; i++) {
            r[i] = results[i].getRows();
        }
        return r;
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkClosed();
        return this.connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        checkClosed();
        return false; // TODO not support
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        checkClosed();
        if (rows == 0) {
            return null;
        }
        return rs;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return (int) executeLargeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        checkNullOrEmpty(sql);
        executeInternal(sql); // TODO autoGeneratedKey
        return rows;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdate(sql, Util.autoGeneratedKey(columnIndexes));
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql, Util.autoGeneratedKey(columnNames));
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        checkNullOrEmpty(sql);
        executeInternal(sql); // TODO autoGeneratedKeys
        return this.rs != null;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql, Util.autoGeneratedKey(columnIndexes));
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql, Util.autoGeneratedKey(columnNames));
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        checkClosed();
        this.poolable = poolable;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        checkClosed();
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
}
