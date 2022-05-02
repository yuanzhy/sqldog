package com.yuanzhy.sqldog.core.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/18
 */
public interface Executor extends Remote, AutoCloseable {

    String getVersion() throws RemoteException;

    Response execute(Request request) throws RemoteException;
    void close() throws RemoteException;
}
