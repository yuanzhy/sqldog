package com.yuanzhy.sqldog.cli;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.math.NumberUtils;

import com.yuanzhy.sqldog.cli.client.SocketClient;
import com.yuanzhy.sqldog.core.SqldogVersion;
import com.yuanzhy.sqldog.core.util.Asserts;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class Main {

    private static final Options OPTIONS = new Options();

    static {
        // help
        //OPTIONS.addOption("help", "usage help");

        //OptionGroup connOptions = new OptionGroup();
        OPTIONS.addOption(Option.builder("h").hasArg(true).longOpt("host").type(String.class).desc("database server host (default: \"127.0.0.1\")").build());
        OPTIONS.addOption(Option.builder("p").hasArg(true).longOpt("port").type(Short.TYPE).desc("database server port (default: \"2345\")").build());
        OPTIONS.addOption(Option.builder("U").required(true).hasArg(true).longOpt("username").type(String.class).desc("database user name ").build());
        //connOptions.addOption(Option.builder("w").hasArg(true).longOpt("no-password").type(String.class).desc("never prompt for password").build());
        OPTIONS.addOption(Option.builder("P").required(true).longOpt("password").hasArg(true).type(String.class).desc("force password prompt (should happen automatically)").build());
        //OPTIONS.addOptionGroup(connOptions);

        //OptionGroup generalOptions = new OptionGroup();
        OPTIONS.addOption(Option.builder("c").hasArg(true).longOpt("command").type(String.class).desc("run only single command (SQL or internal) and exit").build());
        OPTIONS.addOption(Option.builder("d").hasArg(true).longOpt("dbname").type(String.class).desc("database name to connect to").build());
        OPTIONS.addOption(Option.builder("f").hasArg(true).longOpt("file").type(String.class).desc("execute commands from file, then exit").build());
        OPTIONS.addOption(Option.builder("l").longOpt("list").type(String.class).desc("list available schemas, then exit").build());
        OPTIONS.addOption(Option.builder("V").longOpt("version").type(String.class).desc("output version information, then exit").build());
        OPTIONS.addOption(Option.builder("H").longOpt("help").type(String.class).desc("show this help, then exit").build());
        //OPTIONS.addOptionGroup(generalOptions);

    }

    public static void main(String[] args) {
        CommandLineParser cliParser = new DefaultParser();
        try {
            CommandLine cli = cliParser.parse(OPTIONS, args);
            if (cli.hasOption("help")) {
                System.out.println(getHelpString());
            } else if (cli.hasOption("V")) {
                System.out.println(SqldogVersion.getVersionOfEmpty());
            } else if (cli.hasOption("U") || cli.hasOption("P")) {
                Asserts.isTrue(cli.hasOption("U") && cli.hasOption("P"), "用户名密码必须同时存在");
                String port = cli.getOptionValue("p", "2345");
                Asserts.isTrue(NumberUtils.isCreatable(port), "Port invalid format: " + port);
                String host = cli.getOptionValue("h", "127.0.0.1");
                String username = cli.getOptionValue("U");
                String password = cli.getOptionValue("P");
                SocketClient socketClient = new SocketClient();
                if (cli.hasOption("c")) {
                    //socketClient.execute();
                    // TODO execute sql command
                } else if (cli.hasOption("f")) {
                    // TODO execute sql file
                } else if (cli.hasOption("l")) {
                    // TODO list schemas
                } else {
                    // TODO 连接 server
                    //socketClient.connect();
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage() + "\n" + getHelpString());
            System.exit(0);
        }
    }

    /**
     * get string of help usage
     *
     * @return help string
     */
    private static String getHelpString() {
        HelpFormatter helpFormatter = new HelpFormatter();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintWriter printWriter = new PrintWriter(baos);
        helpFormatter.printHelp(printWriter, HelpFormatter.DEFAULT_WIDTH, "dsql --help", null, OPTIONS,
                        HelpFormatter.DEFAULT_LEFT_PAD, HelpFormatter.DEFAULT_DESC_PAD, null);
        printWriter.flush();
        return new String(baos.toByteArray());
    }
}
