package com.yuanzhy.sqldog.cli.util;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/6
 */
public final class CliUtil {
    public static final Options OPTIONS = new Options();

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
    /**
     * get string of help usage
     *
     * @return help string
     */
    public static String getHelpString() {
        HelpFormatter helpFormatter = new HelpFormatter();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintWriter printWriter = new PrintWriter(baos);
        helpFormatter.printHelp(printWriter, HelpFormatter.DEFAULT_WIDTH, "dsql --help", null, OPTIONS,
                HelpFormatter.DEFAULT_LEFT_PAD, HelpFormatter.DEFAULT_DESC_PAD, null);
        printWriter.flush();
        return new String(baos.toByteArray());
    }
}
