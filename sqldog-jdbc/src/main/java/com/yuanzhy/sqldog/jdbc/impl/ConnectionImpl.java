package com.yuanzhy.sqldog.jdbc.impl;

import com.yuanzhy.sqldog.core.constant.Consts;
import com.yuanzhy.sqldog.core.constant.RequestType;
import com.yuanzhy.sqldog.core.service.EmbedService;
import com.yuanzhy.sqldog.core.service.Executor;
import com.yuanzhy.sqldog.core.service.Request;
import com.yuanzhy.sqldog.core.service.Response;
import com.yuanzhy.sqldog.core.service.Service;
import com.yuanzhy.sqldog.core.service.impl.RequestBuilder;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.jdbc.Driver;
import com.yuanzhy.sqldog.jdbc.SQLError;
import com.yuanzhy.sqldog.jdbc.SqldogConnection;

import java.rmi.ConnectException;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.CallableStatement;
import java.sql.Connection;
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
import java.util.ServiceLoader;
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

    private static final int DEFAULT_RESULT_SET_TYPE = ResultSet.TYPE_FORWARD_ONLY;
    private static final int DEFAULT_RESULT_SET_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;

    private static final ExecutorService POOL = Executors.newFixedThreadPool(4);

    private final boolean embed;
    private final String host;
    private final int port;
    private final Properties info;
    private final Set<Statement> openStatements = new HashSet<>();

    private Executor executor;
    private boolean isReconnect = false;

    private String database = "default";
    private String schema;
    private volatile boolean isClosed = false;

    public ConnectionImpl(String filePath, String schema, Properties info) throws SQLException {
        this.embed = true;
        this.host = filePath;
        this.port = -1;
        this.info = info;
        connect();
        setSchema(schema);
    }

    public ConnectionImpl(String host, int port, String schema, Properties info) throws SQLException {
        this.embed = false;
        this.host = host;
        this.port = port;
        this.info = info;
        connect();
        setSchema(schema);
    }

    private void connect() throws SQLException {
        if (this.embed) {
            ServiceLoader<EmbedService> sl = ServiceLoader.load(EmbedService.class);
            try {
                EmbedService service = sl.iterator().next();
                executor = service.connect(host);
            } catch (Exception e) {
                throw SQLError.wrapEx(e);
            }
        } else {
            try {
                Registry registry = LocateRegistry.getRegistry(host, port);
                Service service = (Service) registry.lookup(Consts.SERVER_NAME);
                executor = service.connect(info.getProperty(Driver.USER_PROPERTY_KEY), info.getProperty(Driver.PASSWORD_PROPERTY_KEY));
            } catch (Exception e) {
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
        return new DatabaseMetaDataImpl(this, host, port, schema, info, version);
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
        checkClosed();
        // TODO 未实现
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return Connection.TRANSACTION_NONE; // TODO
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
        return createStatement(resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        checkClosed();
        StatementImpl stmt = new StatementImpl(this, this.schema, resultSetType, resultSetConcurrency, resultSetHoldability);
        openStatements.add(stmt);
        return stmt;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql, resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
            int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkClosed();
        PreparedStatementImpl ps = new PreparedStatementImpl(this, this.schema, sql, resultSetType, resultSetConcurrency, resultSetHoldability);
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
        if (schema != null) {
            schema = schema.trim().toUpperCase();
            execute(new RequestBuilder(RequestType.SIMPLE_QUERY).sqls("SET SEARCH_PATH TO " + schema).build());
            this.schema = schema;
        }

    }

    @Override
    public String getSchema() throws SQLException {
        checkClosed();
        return this.schema;
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkClosed();
        // TODO
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public void checkClosed() throws SQLException {
        if (isClosed) {
            throw new SQLException("No operations allowed after connection closed.");
        }
    }

    @Override
    public void close(Statement statement) {
        if (this.isClosed) {
            return;
        }
        this.openStatements.remove(statement);
    }

    protected SqlResult[] execute(Request request) throws SQLException {
        try {
            Response response = executor.execute(request);
            if (response.isSuccess()) {
                isReconnect = false;
                return response.getResults();
            }
            throw new SQLException(response.getMessage());
        } catch (RemoteException e) {
            if (!isReconnect && (e instanceof NoSuchObjectException || e instanceof ConnectException)) {
                connect();
                isReconnect = true;
                return execute(request);
            }
            throw new SQLException(e);
        }
    }

    @Override
    public SqlResult execute(Statement statement, int offset, String sql) throws SQLException {
        return execute(statement, offset, new String[]{sql})[0];
    }

    @Override
    public SqlResult[] execute(Statement statement, int offset, String... sqls) throws SQLException {
        checkClosed();
        int timeoutSecond = statement.getQueryTimeout();
        if (timeoutSecond > 0) {
            // TODO 超时实现草率，待完善
            Future<SqlResult[]> future = POOL.submit(() -> executeInternal(statement, offset, sqls));
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
                return executeInternal(statement, offset, sqls);
            } catch (Exception e) {
                throw SQLError.wrapEx(e);
            }
        }
    }

    @Override
    public SqlResult prepareExecute(PreparedStatement statement, String preparedId, String preparedSql) throws SQLException {
        Request request = new RequestBuilder(RequestType.PREPARED_QUERY)
                .schema(getSchema()).timeout(statement.getQueryTimeout()).fetchSize(statement.getFetchSize())
                .preparedId(preparedId).sqls(preparedSql)
                .buildPrepared();
        try {
            return execute(request)[0];
        } catch (Exception e) {
            throw SQLError.wrapEx(e);
        }
    }

    @Override
    public SqlResult[] executePrepared(PreparedStatement statement, int offset, String preparedId, String preparedSql, List<Object[]> parameterList) throws SQLException {
        Request request = new RequestBuilder(RequestType.PREPARED_PARAMETER).schema(getSchema())
                .timeout(statement.getQueryTimeout()).fetchSize(statement.getFetchSize()).offset(offset)
                .preparedId(preparedId).sqls(preparedSql).parameters(parameterList)
                .buildPrepared();
        return execute(request);
    }

    private SqlResult[] executeInternal(Statement statement, int offset, String... sqls) throws SQLException {
        Request request = new RequestBuilder(RequestType.SIMPLE_QUERY).schema(getSchema())
                .fetchSize(statement.getFetchSize()).offset(offset).timeout(statement.getQueryTimeout())
                .sqls(sqls).build();
        return execute(request);
    }

}
