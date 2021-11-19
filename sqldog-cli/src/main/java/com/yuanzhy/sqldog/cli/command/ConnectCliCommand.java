package com.yuanzhy.sqldog.cli.command;

import java.rmi.RemoteException;
import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;

import com.yuanzhy.sqldog.core.constant.Consts;
import com.yuanzhy.sqldog.core.rmi.Response;

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
    public void execute() {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print(">>> ");
            String command = this.waitCommand(scanner);
            try {
                if (StringUtils.startsWithAny(command, "quit", "\\q", "exit")) {
                    close();
                    scanner.close();
                    break;
                }
                Response response = executor.execute(command);
                printResponse(response);
            } catch (RemoteException e) {
                printError(e);
                //log.error("command execute error: {}", command, e);
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
