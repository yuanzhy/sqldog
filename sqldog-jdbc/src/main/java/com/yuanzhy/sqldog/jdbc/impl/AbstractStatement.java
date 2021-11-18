package com.yuanzhy.sqldog.jdbc.impl;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;

/**
 *
 * @author yuanzhy
 * @date 2021-11-17
 */
public abstract class AbstractStatement implements Statement {

    private boolean closed = false;

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void close() throws SQLException {
        if (this.isClosed()) {
            return;
        }
        this.closed = true;
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
    }

    protected void checkNum(int num) throws SQLException {
        if (num <= 0) {
            throw new SQLException("number must great than 0");
        }
    }

}
