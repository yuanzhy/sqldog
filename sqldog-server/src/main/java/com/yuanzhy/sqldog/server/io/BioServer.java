package com.yuanzhy.sqldog.server.io;

import com.yuanzhy.sqldog.core.codec.Codec;
import com.yuanzhy.sqldog.core.codec.SerializeCodec;
import com.yuanzhy.sqldog.core.constant.Auth;
import com.yuanzhy.sqldog.core.constant.Consts;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.sql.SqlCommand;
import com.yuanzhy.sqldog.server.sql.SqlParser;
import com.yuanzhy.sqldog.server.sql.parser.DefaultSqlParser;
import com.yuanzhy.sqldog.server.util.ConfigUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
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

    private final SqlParser sqlParser = new DefaultSqlParser();

    private final Codec<SqlResult> codec = new SerializeCodec<>();
    @Override
    public void start() {
        String host = ConfigUtil.getProperty("server.host", "127.0.0.1");
        int port = Integer.parseInt(ConfigUtil.getProperty("server.port", "2345"));
        String username = ConfigUtil.getProperty("server.username");
        String password = ConfigUtil.getProperty("server.password");
        if (StringUtils.isAnyEmpty(username, password)) {
            LOG.error("config 'server.username , server.password' is missing");
            return;
        }
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
        private boolean authenticated = false;
        Handler(Socket socket) {
            this.socket = socket;
        }
        @Override
        public void run() {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));
                 PrintWriter pw = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8"))) {
                while (true) {
                    String params = this.waitCommand(br);
                    if (StringUtils.isEmpty(params)) {
                        continue;
                    } else if (!authenticated) {
                        if (params.startsWith("auth")) {
                            try {
                                doAuth(params);
                                pw.print(Auth.SUCCESS.value());
                                pw.print(Consts.SEPARATOR);
                                pw.print("Welcome to sqldog v1.0.0");
                            } catch (Exception e) {
                                LOG.warn(e.getMessage());
                                pw.println(e.getMessage().contains(Consts.END_CHAR));
                                pw.flush();
                                break;
                            }
                        } else {
                            pw.println(Auth.ILLEGAL.value().contains(Consts.END_CHAR));
                            pw.flush();
                            break;
                        }
                    } else {
                        if ("quit".equalsIgnoreCase(params) || "\\q".equals(params)) {
                            break;
                        }
                        // 执行脚本
                        SqlCommand sqlCommand = sqlParser.parse(params);
                        try {
                            SqlResult result = sqlCommand.execute();
                            pw.print(new String(codec.encode(result)));
                        } catch (RuntimeException e) {
                            LOG.error(e.getMessage(), e);
                            pw.print(e.getMessage());
                        }
                    }
                    pw.println(Consts.END_CHAR);
                    pw.flush();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                //Databases.currSchema(null);
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        private String waitCommand(BufferedReader br) throws IOException {
            StringBuilder sb = new StringBuilder();
            while (true) {
                String line = br.readLine();
                if (line == null) {
                    break;
                }
                sb.append(line);
                if (line.endsWith(Consts.END_CHAR)) {
                    sb.deleteCharAt(sb.length() - 1);
                    break;
                }
                sb.append("\n");
            }
            return sb.toString().trim();
        }

        private void doAuth(String params) {
            String[] arr = params.split(Consts.SEPARATOR);
            if (arr.length != 2) {
                throw new IllegalArgumentException(Auth.ILLEGAL.value());
            }
            // username:xxx,password:123
            String paramUsername = StringUtils.substringBetween(arr[1], "username:", ",password");
            String paramPassword = StringUtils.substringAfter(arr[1], "password:");
            String realUsername = ConfigUtil.getProperty("server.username");
            String realPassword = ConfigUtil.getProperty("server.password");
            if (realUsername.equals(paramUsername) && realPassword.equals(paramPassword)) {
                authenticated = true;
            } else {
                throw new IllegalArgumentException(Auth.FAILURE.value());
            }
        }
    }
}
