package com.yuanzhy.sqldog.server.io;

import com.yuanzhy.sqldog.core.SqldogVersion;
import com.yuanzhy.sqldog.core.constant.Consts;
import com.yuanzhy.sqldog.core.constant.RequestType;
import com.yuanzhy.sqldog.core.rmi.Executor;
import com.yuanzhy.sqldog.core.rmi.PreparedRequest;
import com.yuanzhy.sqldog.core.rmi.RMIServer;
import com.yuanzhy.sqldog.core.rmi.Request;
import com.yuanzhy.sqldog.core.rmi.Response;
import com.yuanzhy.sqldog.core.rmi.impl.ResponseImpl;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.sql.PreparedSqlCommand;
import com.yuanzhy.sqldog.server.sql.SqlCommand;
import com.yuanzhy.sqldog.server.sql.SqlParser;
import com.yuanzhy.sqldog.server.sql.command.SetCommand;
import com.yuanzhy.sqldog.server.sql.parser.DefaultSqlParser;
import com.yuanzhy.sqldog.server.sql.parser.PreparedSqlParser;
import com.yuanzhy.sqldog.server.util.ConfigUtil;
import com.yuanzhy.sqldog.server.util.Databases;
import com.yuanzhy.sqldog.server.util.LRUCache;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.rmi.ConnectException;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ServerNotActiveException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author yuanzhy
 * @date 2021-11-18
 */
public class RmiServer implements Server {
    private static final Logger log = LoggerFactory.getLogger(RmiServer.class);

    private final SqlParser sqlParser = new DefaultSqlParser();
    private final PreparedSqlParser preparedSqlParser = new PreparedSqlParser();
    private final Map<String, Executor> executors = new HashMap<>();
    private final int maxConnections = ConfigUtil.getIntProperty("server.max-connections");

    private volatile int count = 0;
    @Override
    public void start() {
        String host = ConfigUtil.getProperty("server.host", "127.0.0.1");
        int port = Integer.parseInt(ConfigUtil.getProperty("server.port", "2345"));
        String username = ConfigUtil.getProperty("server.username");
        String password = ConfigUtil.getProperty("server.password");
        if (StringUtils.isAnyEmpty(username, password)) {
            log.error("config 'server.username , server.password' is missing");
            return;
        }
        try {
            // 注册远程对象,向客户端提供远程对象服务。
            // 远程对象是在远程服务上创建的，你无法确切地知道远程服务器上的对象的名称，
            // 但是,将远程对象注册到RMI Registry之后,
            // 客户端就可以通过RMI Registry请求到该远程服务对象的stub，
            // 利用stub代理就可以访问远程服务对象了。
            RMIServer remoteHandler = new RMIServerImpl();
            System.setProperty("java.rmi.server.hostname", host);
            Registry registry = LocateRegistry.createRegistry(port);
            registry.bind(Consts.SERVER_NAME, remoteHandler);
            Databases.getDatabase(StorageConst.DEF_DATABASE_NAME); // 触发一下初始化
            log.info("Sqldog server ready");
            // 如果不想再让该对象被继续调用，使用下面一行
            // UnicastRemoteObject.unexportObject(remoteMath, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    synchronized void removeExecutor(String clientHost, int serialNum) {
        Executor executor = executors.remove(clientHost + "_" + serialNum);
        if (executor == null) {
            return;
        }
        try {
            UnicastRemoteObject.unexportObject(executor, true);
            count--;
        } catch (NoSuchObjectException e) {
            log.warn(e.getMessage(), e);
        }
    }

    void checkExceeds() throws RemoteException {
        if (count >= maxConnections) {
            throw new RemoteException("The number of connections exceeds");
        }
    }

    synchronized Executor createExecutor() throws RemoteException {
        try {
            ExecutorImpl executorImpl = new ExecutorImpl(UnicastRemoteObject.getClientHost(), ++count);
            Executor executor = (Executor) UnicastRemoteObject.exportObject(executorImpl, 0);
            executors.put(executorImpl.clientHost + "_" + executorImpl.serialNum, executorImpl);
            return executor;
        } catch (ServerNotActiveException e) {
            throw new RemoteException(e.getMessage(), e);
        }
    }

    private class RMIServerImpl extends UnicastRemoteObject implements RMIServer {

        protected RMIServerImpl() throws RemoteException { }

        @Override
        public Executor connect(String username, String password) throws RemoteException {
            checkExceeds();
            String realUsername = ConfigUtil.getProperty("server.username");
            String realPassword = ConfigUtil.getProperty("server.password");
            if (realUsername.equals(username) && realPassword.equals(password)) {
                return createExecutor();
            } else {
                throw new ConnectException("Connect error, username or password incorrect !");
            }
        }
    }

    private class ExecutorImpl implements Executor {
        private final String clientHost;
        private final int serialNum;
        private final String version;
        private final Map<String, PreparedSqlCommand> preparedSqlCache = new LRUCache<>(50);
//        private String currentSchema = StorageConst.DEF_SCHEMA_NAME;
        ExecutorImpl(String clientHost, int serialNum) {
            this.clientHost = clientHost;
            this.serialNum = serialNum;
            String version = SqldogVersion.getVersion();
            this.version = version == null ? "1.0.0" : version;
            log.info("newConnection: {}_{}", clientHost, serialNum);
        }

        @Override
        public String getVersion() {
            return this.version;
        }

        @Override
        public Response execute(Request request) throws RemoteException {
            if (request.getType() == RequestType.SIMPLE_QUERY) {
                return this.simpleQuery(request);
            } else if (request.getType() == RequestType.PREPARED_QUERY) {
                return this.prepared((PreparedRequest)request);
            } else if (request.getType() == RequestType.PREPARED_PARAMETER) {
                return this.preparedQuery((PreparedRequest)request);
            }
            throw new IllegalArgumentException("Request Type Not Supported: " + request.getType());
        }

        private Response simpleQuery(Request request) {
            try {
                String[] sqls = request.getSql();
                List<SqlResult> results = new ArrayList<>();
                for (int i = 0; i < sqls.length; i++) {
                    String[] arr = sqls[i].split("(;\\s+\n)");
                    for (String sql : arr) {
                        if (StringUtils.isBlank(sql)) {
                            continue;
                        }
                        SqlCommand sqlCommand = sqlParser.parse(sql);
                        sqlCommand.defaultSchema(request.getSchema());
                        SqlResult result = sqlCommand.execute();
//                        if (sqlCommand instanceof SetCommand && result.getSchema() != null) {
//                            this.currentSchema = result.getSchema();
//                        }
                        results.add(result);
                    }
                }
                return new ResponseImpl(true, results.toArray(new SqlResult[0]));
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                return new ResponseImpl(false, e.getMessage());
            }
        }

        private Response prepared(PreparedRequest request) {
            String preparedSql = request.getSql()[0];
            String preparedId = request.getPrepareId();
            try {
                PreparedSqlCommand sqlCommand = preparedSqlCache.computeIfAbsent(preparedId, key -> preparedSqlParser.parse(preparedSql));
                sqlCommand.defaultSchema(request.getSchema());
                SqlResult result = sqlCommand.execute();
                return new ResponseImpl(true, result);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                return new ResponseImpl(false, e.getMessage());
            }
        }

        private Response preparedQuery(PreparedRequest request) {
            Object[][] parameters = request.getParameters();
            try {
                PreparedSqlCommand sqlCommand = preparedSqlCache.computeIfAbsent(request.getPrepareId(), key -> preparedSqlParser.parse(request.getSql()[0]));
                sqlCommand.defaultSchema(request.getSchema());
                SqlResult[] results = new SqlResult[parameters.length];
                for (int i = 0; i < parameters.length; i++) {
                    results[i] = sqlCommand.execute(parameters[i]);
                }
                return new ResponseImpl(true, results);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                return new ResponseImpl(false, e.getMessage());
            }
        }

        @Override
        public void close() {
            for (PreparedSqlCommand sqlCommand : preparedSqlCache.values()) {
                try {
                    sqlCommand.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            preparedSqlCache.clear();
            removeExecutor(clientHost, serialNum);
            log.info("closeConnection: {}_{}", clientHost, serialNum);
        }
    }
}
