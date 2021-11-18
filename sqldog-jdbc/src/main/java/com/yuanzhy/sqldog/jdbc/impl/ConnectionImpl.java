package com.yuanzhy.sqldog.jdbc.impl;

import com.yuanzhy.sqldog.jdbc.SqldogConnection;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Properties;

/**
 *
 * @author yuanzhy
 * @date 2021-11-16
 */
public class ConnectionImpl extends UnsupportedConnection implements SqldogConnection {

    private static final int DEFAULT_RESULT_SET_TYPE = ResultSet.TYPE_FORWARD_ONLY;
    private static final int DEFAULT_RESULT_SET_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;

    private final String host;
    private final int port;
    private final Properties info;

    private String schema;
    private boolean isClosed = true;


    public ConnectionImpl(String host, int port, String schema, Properties info) {
        this.host = host;
        this.port = port;
        this.schema = schema;
        this.info = info;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return createStatement(DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return null;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {

    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return false;
    }

    @Override
    public void commit() throws SQLException {

    }

    @Override
    public void rollback() throws SQLException {

    }

    @Override
    public void close() throws SQLException {
        if (this.isClosed()) {
            return;
        }
//        this.forceClosedReason = reason;
//
//        try {
//            if (!skipLocalTeardown) {
//                if (!getAutoCommit() && issueRollback) {
//                    try {
//                        rollback();
//                    } catch (SQLException ex) {
//                        sqlEx = ex;
//                    }
//                }
//
//                reportMetrics();
//
//                if (getUseUsageAdvisor()) {
//                    if (!calledExplicitly) {
//                        String message = "Connection implicitly closed by Driver. You should call Connection.close() from your code to free resources more efficiently and avoid resource leaks.";
//
//                        this.eventSink.consumeEvent(new ProfilerEvent(ProfilerEvent.TYPE_WARN, "", this.getCatalog(), this.getId(), -1, -1,
//                                System.currentTimeMillis(), 0, Constants.MILLIS_I18N, null, this.pointOfOrigin, message));
//                    }
//
//                    long connectionLifeTime = System.currentTimeMillis() - this.connectionCreationTimeMillis;
//
//                    if (connectionLifeTime < 500) {
//                        String message = "Connection lifetime of < .5 seconds. You might be un-necessarily creating short-lived connections and should investigate connection pooling to be more efficient.";
//
//                        this.eventSink.consumeEvent(new ProfilerEvent(ProfilerEvent.TYPE_WARN, "", this.getCatalog(), this.getId(), -1, -1,
//                                System.currentTimeMillis(), 0, Constants.MILLIS_I18N, null, this.pointOfOrigin, message));
//                    }
//                }
//
//                try {
//                    closeAllOpenStatements();
//                } catch (SQLException ex) {
//                    sqlEx = ex;
//                }
//
//                if (this.io != null) {
//                    try {
//                        this.io.quit();
//                    } catch (Exception e) {
//                    }
//
//                }
//            } else {
//                this.io.forceClose();
//            }
//            if (this.statementInterceptors != null) {
//                for (int i = 0; i < this.statementInterceptors.size(); i++) {
//                    this.statementInterceptors.get(i).destroy();
//                }
//            }
//
//            if (this.exceptionInterceptor != null) {
//                this.exceptionInterceptor.destroy();
//            }
//        } finally {
//            this.openStatements.clear();
//            if (this.io != null) {
//                this.io.releaseResources();
//                this.io = null;
//            }
//            this.statementInterceptors = null;
//            this.exceptionInterceptor = null;
//            ProfilerEventHandlerFactory.removeInstance(this);
//
//            synchronized (getConnectionMutex()) {
//                if (this.cancelTimer != null) {
//                    this.cancelTimer.cancel();
//                }
//            }
//
//            this.isClosed = true;
//        }
//
//        if (sqlEx != null) {
//            throw sqlEx;
//        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    private void checkClosed() throws SQLException {
        if (isClosed) {
            throw new SQLException("No operations allowed after connection closed.");
        }
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {

    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {

    }

    @Override
    public String getCatalog() throws SQLException {
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {

    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return 0;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        checkClosed();

        StatementImpl stmt = new StatementImpl(this, this.schema, resultSetType, resultSetConcurrency);
//        stmt.setResultSetType(resultSetType);
//        stmt.setResultSetConcurrency(resultSetConcurrency);
        return stmt;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return null;
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException {
        return null;
    }

    @Override
    public Clob createClob() throws SQLException {
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return !isClosed;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return null;
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkClosed();
        this.schema = schema;
    }

    @Override
    public String getSchema() throws SQLException {
        checkClosed();
        return this.schema;
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
