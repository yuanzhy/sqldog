package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.core.util.Asserts;
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
    public String execute() {
        // SET search_path TO my_schema;
        String schemaName;
        if (sql.startsWith("SET SEARCH_PATH TO ")) {
            schemaName = sql.substring("SET SEARCH_PATH TO ".length());
        } else if (sql.startsWith("USE ")) {
            schemaName = sql.substring("USE ".length());
        } else {
            throw new UnsupportedOperationException("not supported: " + sql);
        }
        Asserts.hasText(schemaName, "Illegal schema name");
        Databases.currSchema(schemaName);
        return "current " + schemaName;
    }
}