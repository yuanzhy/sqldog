package com.yuanzhy.sqldog.cli;

import com.yuanzhy.sqldog.cli.command.CliCommand;
import com.yuanzhy.sqldog.cli.command.CliCommandFactory;
import com.yuanzhy.sqldog.cli.util.CliUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class Main {

    public static void main(String[] args) {
        CommandLineParser cliParser = new DefaultParser();
        try {
            CommandLine cli = cliParser.parse(CliUtil.OPTIONS, args);
            CliCommand cliCommand = CliCommandFactory.create(cli);
            cliCommand.execute();
        } catch (Exception e) {
            System.out.println(e.getMessage() + "\n" + CliUtil.getHelpString());
            System.exit(0);
        }
    }
}
