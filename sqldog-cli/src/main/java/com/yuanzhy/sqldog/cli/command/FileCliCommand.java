package com.yuanzhy.sqldog.cli.command;

import org.apache.commons.io.FileUtils;

import java.io.File;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/6
 */
public class FileCliCommand extends RemoteCliCommand {
    private final File file;
    public FileCliCommand(String host, int port, String username, String password, String filename) {
        super(host, port, username, password);
        file = new File(filename);
        if (!file.exists()) {
            throw new IllegalArgumentException(filename + " not exists");
        }
        if (!file.isFile()) {
            throw new IllegalArgumentException(filename + " not file");
        }
    }

    @Override
    public void execute() {
        try {
            String text = FileUtils.readFileToString(file, "UTF-8").trim();
            if (text.contains("\r")) {
                text = text.replace("\r", "");
            }
            String[] sqls = text.split(";\n");
            executeAndExit(sqls);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
