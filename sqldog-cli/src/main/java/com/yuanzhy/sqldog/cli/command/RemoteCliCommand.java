package com.yuanzhy.sqldog.cli.command;

import com.yuanzhy.sqldog.cli.util.FormatterUtil;
import com.yuanzhy.sqldog.core.util.DateUtil;
import org.apache.commons.lang3.StringUtils;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/6
 */
public abstract class RemoteCliCommand implements CliCommand, Closeable {

    final int MORE_MOD = 20;

    private final String host;
    private final int port;
    private final String username;
    private final String password;
    protected Connection conn;

    public RemoteCliCommand(String host, int port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.connect();
    }

    protected final void connect() {
        try {
            Class.forName("com.yuanzhy.sqldog.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:sqldog://" + host + ":" + port, username, password);
        } catch (ClassNotFoundException | SQLException e) {
            printError(e);
            throw new RuntimeException("");
        }
    }

    protected final void executeAndExit(String... sql) {
        try {
            execute(Boolean.FALSE, conn.createStatement(), sql);
        } catch (SQLException e) {
            printError(e);
        } finally {
            close();
        }
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
                result[i] = new String((byte[]) value);
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
            conn.close();
        } catch (SQLException e) {
            printError(e);
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

    public void execute(boolean useMore, Statement stmt, String... sqls) throws SQLException {
        for (int i = 0; i < sqls.length; i++) {
            String[] arr = sqls[i].split("(;\\s+\n)");
            for (String sql : arr) {
                if (StringUtils.isBlank(sql)) {
                    continue;
                }
                String tmp = StringUtils.upperCase(sql);
                tmp = tmp.replace(";", "");
                tmp = tmp.replace(" \\s{2,}+", "\\s");
                tmp = tmp.trim();
                String[] tmpArr = tmp.split("\\s");
                DatabaseMetaData dbmd = conn.getMetaData();
                String catalog = conn.getCatalog();
                String schema = conn.getSchema();
                if ("SHOW".equals(tmpArr[0])) {
                    String sqlSuffix = tmpArr[1];
                    if ("DATABASES".equals(sqlSuffix)) {
                        printResultSet(dbmd.getCatalogs(), useMore);
                    } else if ("SCHEMAS".equals(sqlSuffix)) {
                        printResultSet(dbmd.getSchemas(), useMore);
                    } else if ("TABLES".equals(sqlSuffix)) {
                        printResultSet(dbmd.getTables(catalog, schema, null, null), useMore);
                    } else if ("SEARCH_PATH".equals(sqlSuffix)) {
                        System.out.println(schema);
                    } else if ("TABLETYPES".equals(sqlSuffix)) {
                        printResultSet(dbmd.getTableTypes(), useMore);
                    } else if ("TYPEINFO".equals(sqlSuffix)) {
                        printResultSet(dbmd.getTypeInfo(), useMore);
                    } else if ("FUNCTIONS".equals(sqlSuffix)) {
                        printResultSet(dbmd.getFunctions(catalog, schema, null), useMore);
                    } else {
                        System.out.println("not supported: " + sql);
                    }
                } else if (tmp.startsWith("SET CLIENT_ENCODING")) {
                    // 默认都使用UTF-8
                } else if (StringUtils.equalsAny(tmpArr[0], "USE", "SET SEARCH_PATH TO")) {
                    String schemaName = tmpArr[1];
                    conn.setSchema(schemaName);
                    System.out.println(schemaName);
                } else if (StringUtils.equalsAny(tmpArr[0], "\\D", "DESC")) {
                    if (tmpArr.length == 1) { // 说明是\d, 列出所有表
                        printResultSet(dbmd.getTables(catalog, schema, null, null), useMore);
                    } else {
                        String tableName = tmpArr[1];
                        printResultSet(dbmd.getColumns(catalog, schema, tableName, null), useMore);
                        // TODO 输出索引
//                        printResultSet(dbmd.getPrimaryKeys(catalog, schema, tableName), true);
//                        printResultSet(dbmd.getIndexInfo(catalog, schema, tableName, false, false), true);
                    }
                } else {
                    boolean result = stmt.execute(sql);
                    if (result) {
                        printResultSet(stmt.getResultSet(), useMore);
                    } else {
                        int rowCount = stmt.getUpdateCount();
                        System.out.println("SUCCESS (" + rowCount + " rows)");
                    }
                }
            }
        }
//        stmt.close();
    }

    private void printResultSet(ResultSet rs, boolean useMore) throws SQLException {
        if (rs == null) {
            System.out.println("EMPTY RESULTSET");
            return;
        }
        boolean hasData = rs.next();
        if (!hasData) {
            System.out.println("NO DATA");
            return;
        }
        ResultSetMetaData metaData = rs.getMetaData();
        // 获取需要显示列的索引
        List<Integer> showColumnIndex = getShowColumnIndex(metaData);
        List<String> headerList = new ArrayList<>();
        for (int c : showColumnIndex) {
            headerList.add(metaData.getColumnLabel(c));
        }
        String[] headers = headerList.toArray(new String[showColumnIndex.size()]);
        final int LEN = 20;
        FormatterUtil.translateLabel(headers);
        String out = FormatterUtil.joinByVLine(LEN, headers) + "\n" +
                FormatterUtil.genHLine(LEN, headers.length);
        System.out.println(out);
        int rows = 0;
        do {
            rows++;
            List<Object> line = new ArrayList<>();
            for (int c : showColumnIndex) {
                line.add(rs.getObject(c));
            }
            Object[] data = line.toArray();
            System.out.println(toString(data, LEN));

            // 输出MORE
            if (useMore && rows % MORE_MOD == 0) {
                Scanner scanner = new Scanner(System.in);
                System.out.println("-- more -- ");
                String command = scanner.nextLine();
                if (StringUtils.isBlank(command) || " ".equals(command)) {
                    continue;
                }
                if (StringUtils.startsWithAny(command, "q")) {
                    break;
                }
                scanner.close();
            }
        } while (rs.next());
        System.out.println("(total -> " + rows + " rows)");
    }

    private List<Integer> getShowColumnIndex(ResultSetMetaData metaData) throws SQLException {
        // show tables不展示的列 TABLE_CAT,
        String notShowHead = "TYPE_CAT,TYPE_SCHEM,SELF_REFERENCING_COL_NAME,REF_GENERATION";
        // show columns不展示的列
        notShowHead += ",DATA_TYPE,BUFFER_LENGTH,DECIMAL_DIGITS,NUM_PREC_RADIX,COLUMN_DEF,SQL_DATA_TYPE,SQL_DATETIME_SUB," +
                "CHAR_OCTET_LENGTH,ORDINAL_POSITION,NULLABLE,SCOPE_CATALOG,SCOPE_SCHEMA,SCOPE_TABLE,SOURCE_DATA_TYPE,IS_AUTOINCREMENT,IS_GENERATEDCOLUMN";
        List<String> notShowList = Arrays.asList(notShowHead.split(","));
        int count = metaData.getColumnCount();
        List<Integer> showColumnIndex = new ArrayList<>();
        for (int c = 1; c <= count; c++) {
            if (!notShowList.contains(metaData.getColumnLabel(c))) {
                showColumnIndex.add(c);
            }
        }
        return showColumnIndex;
    }
}
