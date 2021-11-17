package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.sql.SqlResultBuilder;

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
        super.parseSchemaTable(sqlSuffix);
        schema.dropTable(table.getName());
        return new SqlResultBuilder(StatementType.DDL).schema(schema.getName()).table(table.getName()).build();
    }
}
