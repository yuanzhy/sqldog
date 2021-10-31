package com.yuanzhy.sqldog.sql.command;

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
    public void execute() {
        // truncate schema.table_name
        String sqlSuffix = sql.substring("TRUNCATE ".length());
        super.parseSchemaTable(sqlSuffix);
        table.truncate();
    }
}
