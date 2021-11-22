package com.yuanzhy.sqldog.server.sql.parser;

import com.yuanzhy.sqldog.server.sql.PreparedSqlCommand;
import com.yuanzhy.sqldog.server.sql.SqlParser;
import com.yuanzhy.sqldog.server.sql.command.prepared.DmlPreparedSqlCommand;
import com.yuanzhy.sqldog.server.sql.command.prepared.SelectPreparedSqlCommand;
import com.yuanzhy.sqldog.server.sql.command.prepared.UpdatePreparedSqlCommand;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/21
 */
public class PreparedSqlParser implements SqlParser {

    @Override
    public PreparedSqlCommand parse(String rawSql) {
        String sql = pre(rawSql);
        if (sql.startsWith("INSERT") || sql.startsWith("DELETE")) {
            return new DmlPreparedSqlCommand(sql);
        } else if (sql.startsWith("SELECT") /*|| sql.startsWith("WITH RECURSIVE")*/) {
            return new SelectPreparedSqlCommand(sql);
        } else if (sql.startsWith("UPDATE")) {
            return new UpdatePreparedSqlCommand(sql);
        }
        throw new UnsupportedOperationException("operation not supported: " + sql);
    }
}
