package com.yuanzhy.sqldog.server.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import com.yuanzhy.sqldog.core.sql.ColumnMetaData;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.sql.SqlCommand;
import com.yuanzhy.sqldog.server.sql.SqlParser;
import com.yuanzhy.sqldog.server.sql.command.SelectCommand;
import com.yuanzhy.sqldog.server.sql.command.SetCommand;
import com.yuanzhy.sqldog.server.sql.command.ShowCommand;
import com.yuanzhy.sqldog.server.sql.parser.DefaultSqlParser;
import com.yuanzhy.sqldog.core.util.ByteUtil;
import com.yuanzhy.sqldog.server.util.ConfigUtil;

/**
 * @author maoning
 * @date 2022/5/1
 */
public class BioServer2 implements Server {
    private final SqlParser sqlParser = new DefaultSqlParser();

    @Override
    public void start() {
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

        private boolean authenticated = false;

        private char separatorChar = '\0';

        private Charset charset = Charset.defaultCharset();

        private String currSchema = "public";

        Handler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try {
                InputStream is = socket.getInputStream();
                OutputStream os = socket.getOutputStream();
                w:
                while (true) {
                    if (authenticated) {
                        // normal
                        boolean isFinish = handleNormalSimpleQueryMessage(is, os);
                        if (isFinish) break;
                    } else {
                        // startup message
                        handleStartupMessage(is, os);
                    }
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

        private void handleStartupMessage(InputStream is, OutputStream os) throws IOException {
            byte[] b1 = new byte[4];
            int index = is.read(b1, 0, 4); // 读取4bytes, 长度
            int len = ByteUtil.toInt(b1);
            System.out.println("消息的长度:" + len);
            index = is.read(b1, 0, 4); // 4bytes, 协议
            int protocol = ByteUtil.toInt(b1);
            System.out.println("协议:" + protocol);
            if (protocol == 80877103) { // SSL
                os.write("N".getBytes());
                System.out.println("不使用SSL");
            } else if (protocol == 196608) { // StartupMessage
                System.out.println("startup");
                byte[] bytes = new byte[len - 4];
                int i = is.read(bytes);
                String msg = new String(bytes);
                System.out.println(msg);
                String[] msgs = StringUtils.split(msg, '\0');
                System.out.println(Arrays.toString(msgs));
                // TODO 输入密码校验, 返回ParamStatus
                // R83 AuthenticationCleartextPassword
                /*os.write(responseBytes("R", 8, 3));*/

                // K12xxxx  BackendKeyData
                /*int threadId = (int) Thread.currentThread().getId();
                os.write(responseBytes("K", 12, threadId));
                os.write("suibiandemiyao".getBytes());*/

                charset = Charset.forName("UTF8");
                System.out.println("设置了客户端编码:" + charset.name());

                // R80 AuthenticationOk
                os.write(responseBytes("R", 8, 0));

                // Z5I ReadyForQuery
                os.write(responseBytes("Z", 5, "I"));
                authenticated = true;
            } else if (protocol == 80877102) { // cancel
                // 取消
                System.out.println("cancel");
            } else {
                // Exxxx
                System.out.println("unknown startup message:" + protocol);
            }
            os.flush();
        }

        private boolean handleNormalSimpleQueryMessage(InputStream is, OutputStream os) throws IOException {
            boolean isFinish = false;
            int firstInt = is.read();
            if (firstInt == -1) {
                System.out.println("读到结尾了...");
                isFinish = true;
                return isFinish;
            }
            byte first = (byte) firstInt;
            System.out.println("请求类型:" + first);
            switch (first) {
                case 'Q':
                    byte[] b1 = new byte[4];
                    int index = is.read(b1, 0, 4); // 读取4bytes, 长度
                    int len = ByteUtil.toInt(b1);
                    byte[] bytes = new byte[len - 4];
                    is.read(bytes);
                    String msg = new String(bytes);
                    System.out.println(msg);
                    String[] msgs = StringUtils.split(msg, '\0');
                    System.out.println(Arrays.toString(msgs));
                    for (String str : msgs) {
                        SqlResult result = null;
                        SqlCommand sqlCommand = sqlParser.parse(str);
                        sqlCommand.defaultSchema(currSchema);
                        try {
                            result = sqlCommand.execute();
                        } catch (RuntimeException e) {
                            System.err.println(e.getMessage());
                            continue;
                        }
                        if (sqlCommand instanceof SetCommand) {
                            currSchema = result.getSchema();
                            System.out.println("当前模式:" + currSchema);
                            os.write(responseBytes("C", 0, result.getSchema(), separatorChar));
                            os.flush();
                        } else if (sqlCommand instanceof SelectCommand
                                || sqlCommand instanceof ShowCommand) {
                            if (result.getColumns() == null) {
                                os.write(responseBytes("C", 0, result.getSchema(), separatorChar));
                                os.flush();
                                continue;
                            }
                            // Txxx RowDescription
                            sendRowDescription(result, os);
                            // Dxxx DataRow
                            sendDataRows(result, os);
                        }

                        // Cxxx CommandComplete
                        String className = sqlCommand.getClass().getName();
                        className = className.toUpperCase();
                        String oper = className.substring(className.lastIndexOf(".") + 1, className.lastIndexOf("COMMAND"));
                        String tip = oper + " " + result.getRows();
                        os.write(responseBytes("C", 0, tip, separatorChar));
                        os.flush();
                    }
                    // Z5I ReadyForQuery
                    os.write(responseBytes("Z", 5, "I"));
                    break;
                case 'X':
                    System.out.println("close");
                    isFinish = true;
                    break;
            }
            return isFinish;
        }

        private void sendRowDescription(SqlResult result, OutputStream os) throws IOException {
            ColumnMetaData[] cmds = result.getColumns();
            List<Object> params = new ArrayList<>();
            params.add((short) cmds.length); // Int16 可以为0
            for (ColumnMetaData cmd : cmds) {
                params.add(cmd.getColumnName()); // String 字段名
                params.add(10); // Int32
                params.add((short) 20); // Int16
                params.add(cmd.getColumnType()); // Int32
                params.add((short) -1); // Int16
                params.add(-4); // Int32
                params.add((short) 0); // Int16 零（文本）或者一（二进制）
                params.add(separatorChar);
            }
            os.write(responseBytes("T", 0, params.toArray()));
            os.flush();
        }

        private void sendDataRows(SqlResult result, OutputStream os) throws IOException {
            List<Object[]> allData = result.getData();
            for (Object[] datas : allData) {
                List<Object> dataParams = new ArrayList<>();
                dataParams.add((short) datas.length); // Int16 可以为0
                for (Object data : datas) {
                    if (data == null) {
                        dataParams.add(convert2Bytes(-1, charset));
                    } else {
                        byte[] bytes = convert2Bytes(data.toString(), charset);
                        dataParams.add(bytes.length);
                        dataParams.add(bytes);
                    }
                }
                os.write(responseBytes("D", 0, dataParams.toArray()));
                os.flush();
            }
        }


        private byte[] responseBytes(String type, int len, Object... params) {
            byte[] types = type.getBytes();
            byte[] paramBytes = null;
            for (Object p : params) {
                paramBytes = ArrayUtils.addAll(paramBytes, convert2Bytes(p, charset));
            }
            int realLen = len == 0 ? paramBytes.length + 4 : len;
            byte[] lens = ByteUtil.toBytes(realLen);
            byte[] result = ArrayUtils.addAll(ArrayUtils.addAll(types, lens), paramBytes);
            return result;
        }

        private byte[] convert2Bytes(Object obj, Charset charset) {
            if (charset == null) {
                charset = Charset.defaultCharset();
            }
            if (obj instanceof String) {
                return ((String) obj).getBytes(charset);
            } else if (obj instanceof Integer) {
                return ByteUtil.toBytes((int) obj);
            } else if (obj instanceof Short) {
                return ByteUtil.toBytes((short) obj);
            } else if (obj instanceof Character) {
                return String.valueOf((char) obj).getBytes();
            } else if (obj instanceof byte[]) {
                return (byte[]) obj;
            } else if (obj instanceof Time) {
                Time t = (Time) obj;
                return t.toString().getBytes();
            } else if (obj instanceof Timestamp) {
                Timestamp tsm = (Timestamp) obj;
                return ByteUtil.toBytes(tsm.getTime());
            }
            // TODO 其他类型待补充
            return null;
        }
    }
}
