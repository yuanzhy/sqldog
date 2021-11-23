package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.sql.result.SqlResultBuilder;

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
    public SqlResult execute() {
        // drop table schema.table_name
        String sqlSuffix = sql.substring("DROP TABLE ".length());
        String tableName = null;
        if (sqlSuffix.startsWith("IF EXISTS ")) {
            try {
                sqlSuffix = sqlSuffix.substring("IF EXISTS ".length());
                super.parseSchemaTable(sqlSuffix);
                schema.dropTable(table.getName());
                tableName = table.getName();
            } catch (Exception e) {
                // ignore
            }
        } else {
            super.parseSchemaTable(sqlSuffix);
            schema.dropTable(table.getName());
            tableName = table.getName();
        }
        return new SqlResultBuilder(StatementType.DDL).schema(schema.getName()).table(tableName).build();
    }
}
