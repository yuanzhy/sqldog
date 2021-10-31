package com.yuanzhy.sqldog.sql.command;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class DropTableCommand extends AbstractSqlCommand {
    public DropTableCommand(String sql) {
        super(sql);
    }

    @Override
    public void execute() {
        // drop table schema.table_name
        String sqlSuffix = sql.substring("DROP TABLE ".length());
        super.parseSchemaTable(sqlSuffix);
        schema.dropTable(table.getName());
    }
}
