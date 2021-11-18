package com.yuanzhy.sqldog.core.rmi;

import java.rmi.Remote;

/**
 *
 * @author yuanzhy
 * @date 2021-11-18
 */
public interface CommandHandler extends Remote {

    Response auth(String username, String password);

    Response quit();

    Response execute(String cmd);
}
