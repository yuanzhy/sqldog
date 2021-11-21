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

    Response execute(String... sqls) throws RemoteException;

    Response prepare(String preparedSql) throws RemoteException;

    Response executePrepared(String preparedSql, Object[]... parameters) throws RemoteException;

    void close() throws RemoteException;
}
