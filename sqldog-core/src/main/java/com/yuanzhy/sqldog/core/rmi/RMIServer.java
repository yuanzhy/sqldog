package com.yuanzhy.sqldog.core.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/18
 */
public interface RMIServer extends Remote {

    Executor connect(String username, String password) throws RemoteException;
}
