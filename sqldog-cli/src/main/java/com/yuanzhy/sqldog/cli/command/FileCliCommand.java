package com.yuanzhy.sqldog.cli.command;

import java.io.File;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.comparator.NameFileComparator;
import org.apache.commons.lang3.ArrayUtils;

import com.yuanzhy.sqldog.core.rmi.Response;

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
    }

    @Override
    public void execute() {
        try {
            recursiveExec(file);
        } catch (Exception e) {
            printError(e);
        } finally {
            close();
        }
    }

    private void recursiveExec(File file) throws Exception {
        if (file.isDirectory()) {
            File[] files = file.listFiles((dir, name) -> name.endsWith(".sql"));
            if (ArrayUtils.isNotEmpty(files)) {
                Arrays.sort(files, NameFileComparator.NAME_COMPARATOR);
                for (File subFile : files) {
                    recursiveExec(subFile);
                }
            }
        } else {
            String text = FileUtils.readFileToString(file, "UTF-8").trim();
            if (text.contains("\r")) {
                text = text.replace("\r", "");
            }
            String[] sqls = text.split(";\n");
            Response response = executor.execute(sqls);
            printResponse(response);
        }
    }
}
