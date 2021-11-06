package com.yuanzhy.sqldog.cli.command;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/6
 */
public class ListCliCommand extends RemoteCliCommand {

    public ListCliCommand(String host, int port, String username, String password) {
        super(host, port, username, password);
    }

    @Override
    protected void executeInternal() {
        executeAndExit("show schemas");
    }
}
