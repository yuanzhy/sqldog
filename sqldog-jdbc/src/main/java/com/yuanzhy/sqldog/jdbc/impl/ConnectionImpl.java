package com.yuanzhy.sqldog.jdbc.impl;

import com.yuanzhy.sqldog.core.constant.Consts;
import com.yuanzhy.sqldog.core.rmi.Executor;
import com.yuanzhy.sqldog.core.rmi.RMIServer;
import com.yuanzhy.sqldog.core.rmi.Response;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.jdbc.Driver;
import com.yuanzhy.sqldog.jdbc.SQLError;
import com.yuanzhy.sqldog.jdbc.SqldogConnection;
import org.apache.commons.lang3.StringUtils;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author yuanzhy
 * @date 2021-11-16
 */
public class ConnectionImpl extends AbstractConnection implements SqldogConnection {

    private static final int DEFAULT_RESULT_SET_TYPE = ResultSet.TYPE_SCROLL_INSENSITIVE;
    private static final int DEFAULT_RESULT_SET_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;

    private static final ExecutorService POOL = Executors.newFixedThreadPool(4);

    private final String host;
    private final int port;
    private final Properties info;
    private final Executor executor;
    private final Set<Statement> openStatements = new HashSet<>();

    private String database = "default";
    private String schema;
    private boolean isClosed = false;


    public ConnectionImpl(String host, int port, String schema, Properties info) throws SQLException {
        this.host = host;
        this.port = port;
        this.schema = schema;
        this.info = info;
        Executor executor = null;
        try {
            Registry registry = LocateRegistry.getRegistry(host, port);
            RMIServer rmiServer = (RMIServer) registry.lookup(Consts.SERVER_NAME);
            executor = rmiServer.connect(info.getProperty(Driver.USER_PROPERTY_KEY), info.getProperty(Driver.PASSWORD_PROPERTY_KEY));
        } catch (Exception e) {
            throw SQLError.wrapEx(e);
        }
        this.executor = executor;
        useSchema();
    }

    protected void useSchema() throws SQLException {
        if (StringUtils.isNotEmpty(schema)) {
            try {
                executor.execute("USE " + schema);
            } catch (RemoteException e) {
                throw SQLError.wrapEx(e);
            }
        }
    }

    @Override
    public Statement createStatement() throws SQLException {
        return createStatement(DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return prepareCall(sql, DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
        // TODO
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return true;
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
        // TODO
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
        // TODO
    }

    @Override
    public void close() throws SQLException {
        if (this.isClosed()) {
            return;
        }
        this.isClosed = true;
        try {
            executor.close();
        } catch (RemoteException ex) {
        }
//        if (this.cancelTimer != null) {
//                    this.cancelTimer.cancel();
//                }
        SQLException sqlEx = null;
        for (Statement stmt : this.openStatements) {
            try {
                stmt.close();
            } catch (SQLException e) {
                sqlEx = e; // throw it later, cleanup all statements first
            }
        }
        openStatements.clear();
        if (sqlEx != null) {
            throw sqlEx;
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        String version;
        try {
            version = executor.getVersion();
        } catch (RemoteException e) {
            version = Driver.VERSION;
        }
        return new DatabaseMetaDataImpl(host, port, schema, info, version);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkClosed();
        // TODO
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException();
//        this.database = catalog;
    }

    @Override
    public String getCatalog() throws SQLException {
        return this.database;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        throw new SQLFeatureNotSupportedException();
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
        openStatements.add(stmt);
        return stmt;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        PreparedStatementImpl ps = new PreparedStatementImpl(this, this.schema, sql, resultSetType, resultSetConcurrency);
        openStatements.add(ps);
        return ps;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        checkClosed();
        PreparedStatementImpl ps = new PreparedStatementImpl(this, this.schema, sql, DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY, autoGeneratedKeys);
        openStatements.add(ps);
        return ps;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return prepareStatement(sql, Util.autoGeneratedKey(columnIndexes));
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException {
        return prepareStatement(sql, Util.autoGeneratedKey(columnNames));
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return !isClosed;
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkClosed();
        this.schema = schema;
        this.useSchema();
    }

    @Override
    public String getSchema() throws SQLException {
        checkClosed();
        return this.schema;
    }

    @Override
    public void checkClosed() throws SQLException {
        if (isClosed) {
            throw new SQLException("No operations allowed after connection closed.");
        }
    }

    @Override
    public SqlResult[] execute(List<String> sqls, int timeoutSecond) throws SQLException {
        checkClosed();
        if (timeoutSecond > 0) {
            // TODO 超时实现草率，待完善
            Future<SqlResult[]> future = POOL.submit(() -> executeInternal(sqls));
            try {
                return future.get(timeoutSecond, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                throw new SQLTimeoutException("timeout");
            } catch (Exception e) {
                throw SQLError.wrapEx(e);
            } finally {
                // 如果该任务已经完成，将没有影响
                // 如果任务正在运行，将因为中断而被取消
                future.cancel(true); // interrupt if running
            }
        } else {
            try {
                return executeInternal(sqls);
            } catch (Exception e) {
                throw SQLError.wrapEx(e);
            }
        }
    }


    @Override
    public SqlResult execute(String sql, int timeoutSecond) throws SQLException {
        checkClosed();
        if (timeoutSecond > 0) {
            // TODO 超时实现草率，待完善
            Future<SqlResult> future = POOL.submit(() -> executeInternal(sql));
            try {
                return future.get(timeoutSecond, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                throw new SQLTimeoutException("timeout");
            } catch (Exception e) {
                throw SQLError.wrapEx(e);
            } finally {
                // 如果该任务已经完成，将没有影响
                // 如果任务正在运行，将因为中断而被取消
                future.cancel(true); // interrupt if running
            }
        } else {
            try {
                return executeInternal(sql);
            } catch (Exception e) {
                throw SQLError.wrapEx(e);
            }
        }
    }

    @Override
    public SqlResult prepareExecute(String preparedSql) throws SQLException {
        try {
            Response response = executor.prepare(preparedSql);
            if (response.isSuccess()) {
                return response.getResult();
            }
            throw new SQLException(response.getMessage());
        } catch (RemoteException e) {
            throw SQLError.wrapEx(e);
        }
    }

    @Override
    public SqlResult[] executePrepared(String preparedSql, List<Object[]> parameterList) throws SQLException {
        // TODO timeout
        try {
            Response response = executor.executePrepared(preparedSql, parameterList.toArray(new Object[0][]));
            if (response.isSuccess()) {
                return response.getResults();
            }
            throw new SQLException(response.getMessage());
        } catch (RemoteException e) {
            throw SQLError.wrapEx(e);
        }
    }

    private SqlResult[] executeInternal(List<String> sqls) {
        try {
            Response response = executor.execute(sqls.toArray(new String[0]));
            if (response.isSuccess()) {
                return response.getResults();
            }
            throw new RuntimeException(response.getMessage());
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        }
    }

    private SqlResult executeInternal(String sql) {
        try {
            Response response = executor.execute(sql);
            if (response.isSuccess()) {
                return response.getResult();
            }
            throw new RuntimeException(response.getMessage());
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        }
    }
}
