package com.yuanzhy.sqldog.cli.command;

import com.yuanzhy.sqldog.core.rmi.Executor;
import com.yuanzhy.sqldog.core.rmi.RMIServer;
import com.yuanzhy.sqldog.core.rmi.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/6
 */
public abstract class RemoteCliCommand implements CliCommand, Closeable {
    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    protected final Executor executor;

    public RemoteCliCommand(String host, int port, String username, String password) {
        // login
        Executor executor;
        try {
            Registry registry = LocateRegistry.getRegistry(host, port);
            RMIServer rmiServer = (RMIServer) registry.lookup("rmiServer");
            executor = rmiServer.connect(username, password);
            System.out.println("Welcome to sqldog " + executor.getVersion());
        } catch (RemoteException | NotBoundException e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
        this.executor = executor;
    }

    private RMIServer createRmiServer(String host, int port) {
        try {
            Registry registry = LocateRegistry.getRegistry(host, port);
            return (RMIServer) registry.lookup("rmiServer");
        } catch (RemoteException | NotBoundException e) {
            throw new RuntimeException(e);
        }
    }

    protected final void executeAndExit(String cmd) {
        try {
            Response response = executor.execute(cmd);
            System.out.println(response.getMessage());
        } catch (RemoteException e) {
            throw new RuntimeException(e);
        } finally {
            close();
        }
    }

    @Override
    public void close() {
        try {
            executor.close();
        } catch (RemoteException e) {
            log.warn(e.getMessage(), e);
        }
    }

//    protected final String send(String cmd) {
//        if (!cmd.endsWith(Consts.END_CHAR)) {
//            cmd = cmd.concat(Consts.END_CHAR);
//        }
//        pw.println(cmd);
//        try {
//            StringBuilder sb = new StringBuilder();
//            while (true) {
//                String line = br.readLine();
//                sb.append(line);
//                if (line.endsWith(Consts.END_CHAR)) {
//                    break;
//                }
//                sb.append("\n");
//            }
//            sb.deleteCharAt(sb.length() - 1);
//            return sb.toString();
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
}
