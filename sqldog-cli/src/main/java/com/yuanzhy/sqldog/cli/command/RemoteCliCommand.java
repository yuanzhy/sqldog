package com.yuanzhy.sqldog.cli.command;

import com.yuanzhy.sqldog.core.constant.Auth;
import com.yuanzhy.sqldog.core.constant.Consts;
import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/6
 */
public abstract class RemoteCliCommand implements CliCommand, Closeable {
    protected final Socket socket;
    protected final BufferedReader br;
    protected final PrintWriter pw;

    public RemoteCliCommand(String host, int port, String username, String password) {
        try {
            socket = new Socket(host, port);
            br = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));
            pw = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8"), true);
            // login
            String r = send("auth" + Consts.SEPARATOR + String.format("username:%s,password:%s;", username, password));
            if (!r.startsWith(Auth.SUCCESS.value())) {
                System.out.println(r);
                throw new IllegalArgumentException(r);
            }
            r = r.substring(0, r.length() - 1);
            r = r.split(Consts.SEPARATOR)[1];
            System.out.println(r);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public final void execute() {
        try {
            this.executeInternal();
        } finally {
            close();
        }
    }

    protected final void executeAndExit(String cmd) {
        try {
            System.out.println(send(cmd));
        } finally {
            close();
        }
    }

    protected final String send(String cmd) {
        if (!cmd.endsWith(Consts.END_CHAR)) {
            cmd = cmd.concat(Consts.END_CHAR);
        }
        pw.println(cmd);
        try {
            StringBuilder sb = new StringBuilder();
            while (true) {
                String line = br.readLine();
                sb.append(line);
                if (line.endsWith(Consts.END_CHAR)) {
                    break;
                }
                sb.append("\n");
            }
            sb.deleteCharAt(sb.length() - 1);
            return sb.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract void executeInternal();

    @Override
    public void close() {
        IOUtils.closeQuietly(br, pw, socket);
    }
}
