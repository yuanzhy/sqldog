package com.yuanzhy.sqldog.server.io;

import com.yuanzhy.sqldog.core.constant.Consts;
import com.yuanzhy.sqldog.core.service.Executor;
import com.yuanzhy.sqldog.core.service.Service;
import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.common.config.Config;
import com.yuanzhy.sqldog.server.common.config.Configs;
import com.yuanzhy.sqldog.server.util.Databases;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.ConnectException;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ServerNotActiveException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author yuanzhy
 * @date 2021-11-18
 */
public class RmiServer implements Server {
    private static final Logger log = LoggerFactory.getLogger(RmiServer.class);
    private static final int TIMEOUT = 1000 * 60 * 30; // 30分钟超时
    private final Map<String, ExecutorImpl> executors = new ConcurrentHashMap<>();
    private final int maxConnections = Configs.get().getIntProperty("server.max-connections");

    private final ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();

    private volatile int count = 0;
    @Override
    public void start() {
        Config config = Configs.get();
        String host = config.getProperty("server.host", "127.0.0.1");
        int port = Integer.parseInt(config.getProperty("server.port", "2345"));
        String username = config.getProperty("server.username");
        String password = config.getProperty("server.password");
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
            Service remoteHandler = new RMIService();
            System.setProperty("java.rmi.server.hostname", host);
            Registry registry = LocateRegistry.createRegistry(port);
            registry.bind(Consts.SERVER_NAME, remoteHandler);
            Databases.getDatabase(StorageConst.DEF_DATABASE_NAME); // 触发一下初始化
            log.info("Sqldog server ready");
            // 添加定时器
            ses.scheduleAtFixedRate(() -> {
                long now = System.currentTimeMillis();
                try {
                    executors.values().stream().filter(executor -> now - executor.lastRequest > TIMEOUT).forEach(executor -> {
                        executor.close();
                    });
                } catch (Exception e) {
                    // ignore
                }
            }, 10, 10, TimeUnit.MINUTES);
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

    private class RMIService extends UnicastRemoteObject implements Service {
        private final String realUsername = Configs.get().getProperty("server.username");
        private final String realPassword = Configs.get().getProperty("server.password");
        protected RMIService() throws RemoteException { }
        @Override
        public Executor connect(String username, String password) throws RemoteException {
            checkExceeds();
            if (realUsername.equals(username) && realPassword.equals(password)) {
                return createExecutor();
            } else {
                throw new ConnectException("Connect error, username or password incorrect !");
            }
        }
    }

    private class ExecutorImpl extends AbstractExecutor implements Executor {
        private final String clientHost;
        ExecutorImpl(String clientHost, int serialNum) {
            super(serialNum);
            this.clientHost = clientHost;
            log.info("newConnection: {}_{}", clientHost, serialNum);
        }

        @Override
        public void close() {
            super.close();
            removeExecutor(clientHost, serialNum);
            log.info("closeConnection: {}_{}", clientHost, serialNum);
        }
    }
}
