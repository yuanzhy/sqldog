package com.yuanzhy.sqldog.server.io;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yuanzhy.sqldog.core.rmi.CommandHandler;
import com.yuanzhy.sqldog.core.rmi.Response;
import com.yuanzhy.sqldog.server.util.ConfigUtil;

/**
 *
 * @author yuanzhy
 * @date 2021-11-18
 */
public class RmiServer implements Server {
    private static final Logger log = LoggerFactory.getLogger(BioServer.class);
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
            CommandHandler remoteHandler = new CommandHandlerImpl();
            LocateRegistry.createRegistry(port);
            Registry registry = LocateRegistry.getRegistry();
            registry.bind("handler", remoteHandler);
            log.info("Sqldog server ready");
            // 如果不想再让该对象被继续调用，使用下面一行
            // UnicastRemoteObject.unexportObject(remoteMath, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private class CommandHandlerImpl extends UnicastRemoteObject implements CommandHandler {

        protected CommandHandlerImpl() throws RemoteException { }

        @Override
        public Response auth(String username, String password) {
            System.out.println("auth");
            return null;
        }

        @Override
        public Response quit() {
            System.out.println("quit");
            return null;
        }

        @Override
        public Response execute(String cmd) {
            System.out.println("cmd");
            return null;
        }
    }
}
