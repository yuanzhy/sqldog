package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.server.sql.result.SqlResultBuilder;
import com.yuanzhy.sqldog.server.util.Databases;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/6
 */
public class SetCommand extends AbstractSqlCommand {
    public SetCommand(String sql) {
        super(sql);
    }

    @Override
    public SqlResult execute() {
        // SET search_path TO my_schema;
        String schemaName;
        if (sql.startsWith("SET SEARCH_PATH TO ")) {
            schemaName = sql.substring("SET SEARCH_PATH TO ".length());
        } else if (sql.startsWith("USE ")) {
            schemaName = sql.substring("USE ".length());
        } else if (sql.startsWith("SET CLIENT_ENCODING")) {
            // 默认都使用UTF-8
            return new SqlResultBuilder(StatementType.OTHER).build();
        } else {
            throw new UnsupportedOperationException("not supported: " + sql);
        }
        Asserts.hasText(schemaName, "Illegal schema name");
        Asserts.notNull(Databases.getDefault().getSchema(schemaName), "'" + schemaName + "' not exists");
        return new SqlResultBuilder(StatementType.SWITCH_SCHEMA).schema(schemaName).build();
    }
}