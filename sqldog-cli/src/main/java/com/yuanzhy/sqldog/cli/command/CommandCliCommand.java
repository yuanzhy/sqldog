package com.yuanzhy.sqldog.cli.command;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/6
 */
public class CommandCliCommand extends RemoteCliCommand {

    private final String sql;

    public CommandCliCommand(String host, int port, String username, String password, String command) {
        super(host, port, username, password);
        this.sql = command;
    }

    @Override
    protected void executeInternal() {
        executeAndExit(sql);
    }
}
