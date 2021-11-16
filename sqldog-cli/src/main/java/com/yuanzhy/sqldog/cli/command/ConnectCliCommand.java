package com.yuanzhy.sqldog.cli.command;

import com.yuanzhy.sqldog.core.constant.Consts;

import java.util.Scanner;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/6
 */
public class ConnectCliCommand extends RemoteCliCommand {

    public ConnectCliCommand(String host, int port, String username, String password) {
        super(host, port, username, password);
    }

    @Override
    protected void executeInternal() {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print(">>> ");
            String command = this.waitCommand(scanner);
            String res = send(command);
            System.out.println(res);
            if ("quit".equalsIgnoreCase(command) || "\\q".equals(command)) {
                close();
                break;
            }
        }
    }

    private String waitCommand(Scanner scanner) {
        StringBuilder sb = new StringBuilder();
        while (true) {
            String line = scanner.nextLine();
            sb.append(line);
            if (line.endsWith(Consts.END_CHAR)) {
                break;
            }
            sb.append("\n");
            System.out.print("> ");
        }
        return sb.toString();
    }
}
