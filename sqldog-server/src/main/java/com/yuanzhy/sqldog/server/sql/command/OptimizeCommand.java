package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.common.config.Configs;
import com.yuanzhy.sqldog.server.sql.result.SqlResultBuilder;

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
        if (Configs.get().isMemory()) {
            return new SqlResultBuilder(StatementType.OTHER).build();
        }
        // optimize schema abc;
        // optimize table abc.test;
        String tmp = sql.substring("OPTIMIZE ".length());
        if (tmp.startsWith("SCHEMA")) {
            super.parseSchema(tmp);
            for (String tableName : currSchema().getTableNames()) {
                currSchema().getTable(tableName).getTableData().optimize();
            }
            return new SqlResultBuilder(StatementType.OTHER).schema(currSchema().getName()).build();
        } else if (tmp.startsWith("TABLE")) {
            super.parseSchemaTable(tmp);
            table.getTableData().optimize();
            return new SqlResultBuilder(StatementType.OTHER).schema(currSchema().getName()).table(table.getName()).build();
        } else {
            throw new UnsupportedOperationException("Not supported: " + sql);
        }
    }
}
