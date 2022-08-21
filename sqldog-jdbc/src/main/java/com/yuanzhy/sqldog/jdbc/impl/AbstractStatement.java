package com.yuanzhy.sqldog.jdbc.impl;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicBoolean;

import com.yuanzhy.sqldog.jdbc.SqldogConnection;

/**
 *
 * @author yuanzhy
 * @date 2021-11-17
 */
abstract class AbstractStatement extends AbstractWrapper implements Statement {

    protected boolean closed = false;
    private final AtomicBoolean executing = new AtomicBoolean(false);
    protected final SqldogConnection connection;

    AbstractStatement(SqldogConnection connection) {
        this.connection = connection;
    }


    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    protected void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("No operations allowed after statement closed.");
        }
        connection.checkClosed();
    }

    protected void checkNum(int num) throws SQLException {
        if (num <= 0) {
            throw new SQLException("number must great than 0");
        }
    }

    protected void checkNullOrEmpty(String sql) throws SQLException {
        if (sql == null) {
            throw new SQLException("Can not issue NULL sql");
        }
        if (sql.length() == 0) {
            throw new SQLException("Can not issue empty sql");
        }
    }

    protected void beforeExecute() throws SQLException {
        if (!executing.compareAndSet(false, true)) {
            throw new SQLException("Can not concurrent execute");
        }
    }

    protected void afterExecute() {
        executing.compareAndSet(true, false);
    }
}
