package com.yuanzhy.sqldog.server.io;

import com.yuanzhy.sqldog.core.SqlCommand;
import com.yuanzhy.sqldog.core.SqlParser;
import com.yuanzhy.sqldog.sql.parser.DefaultSqlParser;
import com.yuanzhy.sqldog.util.ConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class BioServer implements Server {

    private static final Logger LOG = LoggerFactory.getLogger(BioServer.class);

    private SqlParser sqlParser = new DefaultSqlParser();
    @Override
    public void start() {
        String host = ConfigUtil.getProperty("server.host", "127.0.0.1");
        int port = Integer.parseInt(ConfigUtil.getProperty("server.port", "2345"));
        try {
            ServerSocket ss = new ServerSocket(port);
            while (true) {
                Socket s = ss.accept();
                new Thread(new Handler(s)).start();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private class Handler implements Runnable {

        private final Socket socket;
        Handler(Socket socket) {
            this.socket = socket;
        }
        @Override
        public void run() {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 PrintWriter pw = new PrintWriter(socket.getOutputStream())) {
                while (true) {
                    String command = br.readLine();
                    if (command.equalsIgnoreCase("quit")) {
                        break;
                    }
                    SqlCommand sqlCommand = sqlParser.parse(command);
                    try {
                        sqlCommand.execute();
                        pw.println("executed");
                    } catch (RuntimeException e) {
                        LOG.error(e.getMessage(), e);
                        pw.println(e.getMessage());
                    }
                    pw.flush();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
