package com.yuanzhy.sqldog.cli.command;

import com.yuanzhy.sqldog.core.constant.Consts;
import com.yuanzhy.sqldog.core.rmi.Response;
import org.apache.commons.lang3.StringUtils;

import java.rmi.RemoteException;
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
    public void execute() {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print(">>> ");
            String command = this.waitCommand(scanner);
            try {
                Response response = executor.execute(command); // TODO 格式化输出结果
                System.out.println(response.getMessage());
                if (StringUtils.equalsAny(command, "quit", "\\q", "exit")) {
                    close();
                    scanner.close();
                    break;
                }
            } catch (RemoteException e) {
                log.error("command execute error: {}", command, e);
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
