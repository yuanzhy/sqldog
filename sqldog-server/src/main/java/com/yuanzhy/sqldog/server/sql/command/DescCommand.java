package com.yuanzhy.sqldog.server.sql.command;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/7
 */
public class DescCommand extends AbstractSqlCommand {
    public DescCommand(String sql) {
        super(sql);
    }

    @Override
    public String execute() {
        // \d TABLE_NAME
        String sqlSuffix = sql.startsWith("\\D") ? sql.substring("\\D ".length()) : sql.substring("DESC ".length());
        super.parseSchemaTable(sqlSuffix);
        return table.toPrettyString();
    }
}
