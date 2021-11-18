package com.yuanzhy.sqldog.server.io;

import com.yuanzhy.sqldog.core.rmi.Executor;
import com.yuanzhy.sqldog.core.rmi.RMIServer;
import com.yuanzhy.sqldog.core.rmi.Response;
import com.yuanzhy.sqldog.core.rmi.ResponseImpl;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.core.SqlCommand;
import com.yuanzhy.sqldog.server.core.SqlParser;
import com.yuanzhy.sqldog.server.sql.parser.DefaultSqlParser;
import com.yuanzhy.sqldog.server.util.ConfigUtil;
import com.yuanzhy.sqldog.server.util.Databases;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.ConnectException;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

/**
 *
 * @author yuanzhy
 * @date 2021-11-18
 */
public class RmiServer implements Server {
    private static final Logger log = LoggerFactory.getLogger(RmiServer.class);

    private SqlParser sqlParser = new DefaultSqlParser();
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
            Registry registry = LocateRegistry.createRegistry(port);
            registry.bind("rmiServer", remoteHandler);
            log.info("Sqldog server ready");
            // 如果不想再让该对象被继续调用，使用下面一行
            // UnicastRemoteObject.unexportObject(remoteMath, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private class RMIServerImpl extends UnicastRemoteObject implements RMIServer {

        protected RMIServerImpl() throws RemoteException { }

        @Override
        public Executor connect(String username, String password) throws RemoteException {
            String realUsername = ConfigUtil.getProperty("server.username");
            String realPassword = ConfigUtil.getProperty("server.password");
            if (realUsername.equals(username) && realPassword.equals(password)) {
                ExecutorImpl executor = new ExecutorImpl();
                return (Executor) UnicastRemoteObject.exportObject(executor, 0);
            } else {
                throw new ConnectException("Authentication failure");
            }
        }
    }

    private class ExecutorImpl implements Executor {

        @Override
        public String getVersion() {
            return "1.0.0";
        }

        @Override
        public Response execute(String cmd) {
            SqlCommand sqlCommand = sqlParser.parse(cmd);
            try {
                SqlResult result = sqlCommand.execute();
                return new ResponseImpl(true, result);
            } catch (Exception e) {
                return new ResponseImpl(false, e.getMessage());
            }
        }

        @Override
        public void close() throws NoSuchObjectException {
            Databases.currSchema(null);
            UnicastRemoteObject.unexportObject(this, true);
        }
    }
}
