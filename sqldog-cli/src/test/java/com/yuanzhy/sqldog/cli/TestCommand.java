package com.yuanzhy.sqldog.cli;

import com.yuanzhy.sqldog.cli.command.CommandCliCommand;
import com.yuanzhy.sqldog.cli.command.ListCliCommand;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/6
 */
public class TestCommand {
    private static final String H = "127.0.0.1";
    private static final int P = 2345;
    private static final String U = "root";
    private static final String PW = "123456";
    public static void main(String[] args) {
        new CommandCliCommand(H, P, U, PW, "desc test.t").execute();
        new CommandCliCommand(H, P, U, PW, "create schema haha").execute();
        new CommandCliCommand(H, P, U, PW, "create schema hehe").execute();
        new ListCliCommand(H, P, U, PW).execute();
//        new ConnectCliCommand(H, P, U, PW).execute();
    }
}
