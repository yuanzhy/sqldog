package com.yuanzhy.sqldog.server.sql.command;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class TruncateTableCommand extends AbstractSqlCommand {

    public TruncateTableCommand(String sql) {
        super(sql);
    }

    @Override
    public String execute() {
        // truncate schema.table_name
        String sqlSuffix = sql.substring("TRUNCATE ".length());
        super.parseSchemaTable(sqlSuffix);
        table.truncate();
        return success();
    }
}
