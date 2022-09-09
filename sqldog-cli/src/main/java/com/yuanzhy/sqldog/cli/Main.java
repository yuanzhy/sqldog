package com.yuanzhy.sqldog.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;

import com.yuanzhy.sqldog.cli.command.CliCommand;
import com.yuanzhy.sqldog.cli.command.CliCommandFactory;
import com.yuanzhy.sqldog.cli.util.CliUtil;

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
            if (e instanceof IllegalArgumentException) {
                System.out.println(e.getMessage() + "\n\n" + CliUtil.getHelpString());
            } else {
                System.out.println(e.getMessage() + "\n");
            }
            System.exit(0);
        }
    }
}
