package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.sql.result.SqlResultBuilder;

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
    public SqlResult execute() {
        // truncate schema.table_name
        String sqlSuffix = sql.substring("TRUNCATE TABLE ".length());
        super.parseSchemaTable(sqlSuffix);
        table.getTableData().truncate();
        return new SqlResultBuilder(StatementType.DDL).schema(schema.getName()).table(table.getName()).build();
    }
}
