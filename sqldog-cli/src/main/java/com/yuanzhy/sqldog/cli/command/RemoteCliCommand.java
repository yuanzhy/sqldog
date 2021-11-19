package com.yuanzhy.sqldog.cli.command;

import java.io.Closeable;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yuanzhy.sqldog.cli.util.FormatterUtil;
import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.rmi.Executor;
import com.yuanzhy.sqldog.core.rmi.RMIServer;
import com.yuanzhy.sqldog.core.rmi.Response;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.core.util.DateUtil;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/6
 */
public abstract class RemoteCliCommand implements CliCommand, Closeable {
    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    protected final Executor executor;

    public RemoteCliCommand(String host, int port, String username, String password) {
        // login
        Executor executor;
        try {
            Registry registry = LocateRegistry.getRegistry(host, port);
            RMIServer rmiServer = (RMIServer) registry.lookup("rmiServer");
            executor = rmiServer.connect(username, password);
            System.out.println("Welcome to sqldog " + executor.getVersion());
        } catch (RemoteException | NotBoundException e) {
            printError(e);
            throw new RuntimeException("");
        }
        this.executor = executor;
    }

    protected final void executeAndExit(String cmd) {
        try {
            Response response = executor.execute(cmd);
            printResponse(response);
        } catch (RemoteException e) {
            printError(e);
        } finally {
            close();
        }
    }

    protected void printResponse(Response response) {
        if (StringUtils.isNotEmpty(response.getMessage())) {
            System.out.println(response.getMessage());
        }
        SqlResult result = response.getResult();
        if (result == null) {
            return;
        }
        // print result
        if (result.getType() == StatementType.DDL) {
            System.out.println("SUCCESS");
        } else if (result.getType() == StatementType.DML) {
            System.out.println("(" + result.getRows() + " rows)");
        } else if (result.getType() == StatementType.DCL) {
            // 暂未实现
            System.out.println();
        } else if (result.getType() == StatementType.DTL) {
            // 暂未实现
            System.out.println();
        } else if (result.getType() == StatementType.DQL) {
            // TODO 优化大数据量分页展示功能
            String[] headers = result.getHeaders();
            List<Object[]> data = result.getData();
            final int LEN = 20;
            String out = FormatterUtil.joinByVLine(LEN, headers) + "\n" +
                    FormatterUtil.genHLine(LEN, headers.length) + "\n" +
                    data.stream().map(o -> toString(o, LEN)).collect(Collectors.joining("\n")) + "\n\n" +
                    "(" + result.getRows() + " rows)";
            System.out.println(out);
        } else { // other
            String[] headers = result.getHeaders();
            List<Object[]> data = result.getData();
            if (data == null) {
                String out = result.getSchema();
                if (StringUtils.isNotEmpty(result.getTable())) {
                    out += "." + result.getTable();
                }
                System.out.println(out);
            } else if (headers == null) {
                System.out.println(data.get(0)[0]);
            } else {
                final int LEN = 15;
                String out = FormatterUtil.joinByVLine(LEN, headers) + "\n" +
                        FormatterUtil.genHLine(LEN, headers.length) + "\n" +
                        data.stream().map(o -> toString(o, LEN)).collect(Collectors.joining("\n")) + "\n";
                System.out.println(out);
                // TODO 显示约束信息
                //"Constraint:\n" +
                //        "    " + primaryKey.toPrettyString() + "\n" +
                //        constraint.stream().map(Constraint::toPrettyString).map(s -> "    " + s).collect(
                //                Collectors.joining("\n"))
            }

            /*
                       List of databases
                 Database |    Name    | Description
                ----------+------------+--------------
                 default | schema1    | 模式1
                 default | schema2    | 模式2
             */

            /*
                       List of relations
                 Schema |    Name    | Type  | Description
                --------+------------+-------+----------
                 public | company    | table | postgres
                 public | department | table | postgres
             */

            /*
                                      Table "public.company"
                 Column  |     Type      |  Nullable | Default | Description
                ---------+---------------+-----------+----------+------------
                 id      | integer       |  not null |          |
                 name    | text          |  not null |          |
                 age     | integer       |  not null |          |
                 address | character(50) |           |          |
                 salary  | real          |           |          |
                Indexes:
                    "company_pkey" PRIMARY KEY, btree (id)
             */

        }
    }

    private String toString(Object[] values, int maxLen) {
        String[] result = new String[values.length];
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            if (value instanceof Date) {
                result[i] = DateUtil.formatSqlDate((Date) value);
            } else if (value instanceof Time) {
                result[i] = DateUtil.formatTime((Time) value);
            } else if (value instanceof Timestamp) {
                result[i] = DateUtil.formatTimestamp((Timestamp) value);
            } else if (value instanceof byte[]) {
                result[i] = Base64.getEncoder().encodeToString((byte[]) value);
            } else if (value instanceof Object[]) {
                result[i] = Arrays.toString((Object[]) value);
            } else {
                result[i] = String.valueOf(value);
            }
        }
        return FormatterUtil.joinByVLine(maxLen, result);
    }

    protected void printError(Exception e) {
        String msg;
        if (e.getCause() != null) {
            msg = e.getCause().getMessage();
        } else {
            msg = e.getMessage();
        }
        System.out.println(msg);
    }

    @Override
    public void close() {
        try {
            executor.close();
        } catch (RemoteException e) {
            log.warn(e.getMessage(), e);
        }
    }

//    protected final String send(String cmd) {
//        if (!cmd.endsWith(Consts.END_CHAR)) {
//            cmd = cmd.concat(Consts.END_CHAR);
//        }
//        pw.println(cmd);
//        try {
//            StringBuilder sb = new StringBuilder();
//            while (true) {
//                String line = br.readLine();
//                sb.append(line);
//                if (line.endsWith(Consts.END_CHAR)) {
//                    break;
//                }
//                sb.append("\n");
//            }
//            sb.deleteCharAt(sb.length() - 1);
//            return sb.toString();
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
}
