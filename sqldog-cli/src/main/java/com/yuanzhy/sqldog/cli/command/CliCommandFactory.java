package com.yuanzhy.sqldog.cli.command;

import com.yuanzhy.sqldog.cli.util.CliUtil;
import com.yuanzhy.sqldog.server.core.SqldogVersion;
import com.yuanzhy.sqldog.server.core.util.Asserts;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.math.NumberUtils;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/6
 */
public final class CliCommandFactory {

    private static final CliCommand HELP_COMMAND = () -> System.out.println(CliUtil.getHelpString());
    private static final CliCommand VERSION_COMMAND = () -> System.out.println(SqldogVersion.getVersionOfEmpty());

    public static CliCommand create(CommandLine cli) {
        if (cli.getArgList().size() > 0) {
            throw new IllegalArgumentException("Unknown params: " + cli.getArgList().toString());
        }
        if (cli.hasOption("help")) {
            return HELP_COMMAND;
        } else if (cli.hasOption("V")) {
            return VERSION_COMMAND;
        } else if (cli.hasOption("U") || cli.hasOption("P")) {
            Asserts.isTrue(cli.hasOption("U") && cli.hasOption("P"), "用户名密码必须同时存在");
            String strPort = cli.getOptionValue("p", "2345");
            Asserts.isTrue(NumberUtils.isCreatable(strPort), "Port invalid format: " + strPort);
            String host = cli.getOptionValue("h", "127.0.0.1");
            int port = Integer.parseInt(strPort);
            String username = cli.getOptionValue("U");
            String password = cli.getOptionValue("P");
            RemoteCliCommand cliCommand;
            if (cli.hasOption("c")) {
                cliCommand = new CommandCliCommand(host, port, username, password, cli.getOptionValue("c"));
            } else if (cli.hasOption("f")) {
                cliCommand = new FileCliCommand(host, port, username, password, cli.getOptionValue("f"));
            } else if (cli.hasOption("l")) {
                cliCommand = new ListCliCommand(host, port, username, password);
            } else {
                cliCommand = new ConnectCliCommand(host, port, username, password);
            }
            return cliCommand;
        } else {
            throw new IllegalArgumentException("Unknown Command");
        }
    }
}
