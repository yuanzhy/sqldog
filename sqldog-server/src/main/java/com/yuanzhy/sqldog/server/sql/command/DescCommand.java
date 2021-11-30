package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.sql.result.SqlResultBuilder;

import java.util.stream.Collectors;

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
    public SqlResult execute() {
        // \d TABLE_NAME
        String sqlSuffix = sql.startsWith("\\D") ? sql.substring("\\D ".length()) : sql.substring("DESC ".length());
        super.parseSchemaTable(sqlSuffix);
        return new SqlResultBuilder(StatementType.OTHER).schema(schema.getName()).table(table.getName())
                .labels("Column", "Type", "Nullable", "Default", "Description")
                .data(table.getColumns().values().stream().map(c -> new Object[]{c.getName(), c.getDataType().name(), c.isNullable() ? "NULL" : "NOT NULL", c.defaultValue(), c.getDescription()}).collect(Collectors.toList()))
                .constraints(table.getConstraints())
                .build();
    }
}
