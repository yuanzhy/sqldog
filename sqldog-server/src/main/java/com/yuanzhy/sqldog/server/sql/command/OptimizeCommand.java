package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.sql.result.SqlResultBuilder;
import com.yuanzhy.sqldog.server.util.ConfigUtil;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/5/1
 */
public class OptimizeCommand extends AbstractSqlCommand {

    public OptimizeCommand(String sql) {
        super(sql);
    }

    @Override
    public SqlResult execute() {
        if (ConfigUtil.isMemory()) {
            return new SqlResultBuilder(StatementType.OTHER).build();
        }
        // optimize schema abc;
        // optimize table abc.test;
        String tmp = sql.substring("OPTIMIZE ".length());
        if (tmp.startsWith("SCHEMA")) {
            super.parseSchema(tmp);
            for (String tableName : schema.getTableNames()) {
                schema.getTable(tableName).getTableData().optimize();
            }
            return new SqlResultBuilder(StatementType.OTHER).schema(schema.getName()).build();
        } else if (tmp.startsWith("TABLE")) {
            super.parseSchemaTable(tmp);
            table.getTableData().optimize();
            return new SqlResultBuilder(StatementType.OTHER).schema(schema.getName()).table(table.getName()).build();
        } else {
            throw new UnsupportedOperationException("Not supported: " + sql);
        }
    }
}
