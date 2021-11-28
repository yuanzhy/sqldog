package com.yuanzhy.sqldog.server.sql.command.prepared;

import com.yuanzhy.sqldog.core.constant.Consts;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.core.util.DateUtil;
import com.yuanzhy.sqldog.server.sql.PreparedSqlCommand;
import com.yuanzhy.sqldog.server.sql.SqlCommand;
import com.yuanzhy.sqldog.server.sql.command.DeleteCommand;
import com.yuanzhy.sqldog.server.sql.command.InsertCommand;
import com.yuanzhy.sqldog.server.util.Databases;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/21
 */
public class DmlPreparedSqlCommand extends AbstractPreparedSqlCommand implements PreparedSqlCommand {

    public DmlPreparedSqlCommand(String preparedSql) {
        super(preparedSql);
    }

    @Override
    public SqlResult execute(Object[] parameter) {
        String sql = replacePlaceholder(parameter);
        return this.getDelegateCommand(sql).execute();
    }

    private SqlCommand getDelegateCommand(String sql) {
        SqlCommand command;
        // TODO 先粗暴的实现一下，后续改为直接拿值并调用 table.getDML().xxx()
        if (sql.startsWith("INSERT")) {
            command = new InsertCommand(sql);
        } else if (sql.startsWith("DELETE")) {
            command = new DeleteCommand(sql);
        } else {
            throw new UnsupportedOperationException("not supported: " + sql);
        }
        command.currentSchema(Databases.currSchema());
        return command;
    }

    private String replacePlaceholder(Object[] parameter) {
        boolean valueToken = false;
        boolean escape = false;
        int count = 0;
        StringBuilder sb = new StringBuilder();
        for (char c : preparedSql.toCharArray()) {
            // --- 转义处理 ---
            if (c == Consts.SQL_ESCAPE) {
                escape = true;
                sb.append(c);
                continue;
            }
            if (!escape && c == Consts.SQL_QUOTES) {
                valueToken = !valueToken;
                sb.append(c);
            } else if (!valueToken && c == Consts.SQL_QUESTION_MARK) {
                Object param = parameter[count++];
                sb.append(toString(param));
            } else {
                sb.append(c);
            }
            escape = false;
        }
        return sb.toString();
    }


    private String toString(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof Date) {
            return "'" + DateUtil.formatSqlDate((Date) value) + "'";
        } else if (value instanceof Time) {
            return "'" + DateUtil.formatTime((Time) value) + "'";
        } else if (value instanceof Timestamp) {
            return "'" + DateUtil.formatTimestamp((Timestamp) value) + "'";
        } else if (value instanceof byte[]) {
            return "'" + new String((byte[]) value) + "'";
        } else if (value instanceof Object[]) {
            return Arrays.toString((Object[]) value);
        } else if (value instanceof String) {
            return "'" + value + "'";
        } else {
            return String.valueOf(value);
        }
    }
}
