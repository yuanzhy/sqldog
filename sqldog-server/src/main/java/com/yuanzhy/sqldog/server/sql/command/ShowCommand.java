package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.sql.result.SqlResultBuilder;
import com.yuanzhy.sqldog.server.util.Databases;

import java.util.stream.Collectors;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/6
 */
public class ShowCommand extends AbstractSqlCommand {
    public ShowCommand(String sql) {
        super(sql);
    }

    @Override
    public SqlResult execute() {
        String sqlSuffix = sql.substring("SHOW ".length());
        SqlResultBuilder builder = new SqlResultBuilder(StatementType.OTHER);
        String dbName = Databases.getDefault().getName();
        if ("DATABASES".equals(sqlSuffix)) {
            return builder.data(dbName).build();
        } else if ("SCHEMAS".equals(sqlSuffix)) {
            return builder.labels("Database", "Name", "Description")
                    .data(Databases.getDefault().getSchemaNames().stream().map(s -> {
                        Schema schema = Databases.getDefault().getSchema(s);
                        return new Object[]{dbName, schema.getName(), schema.getDescription()}; }).collect(
                            Collectors.toList()))
                    .build();
        } else if ("TABLES".equals(sqlSuffix)) {
            checkSchema();
            return builder.labels("Schema", "Name", "Type", "Description")
                    .schema(schema.getName())
                    .data(schema.getTableNames().stream().map(t -> {
                        Table table = schema.getTable(t);
                        return new Object[]{schema.getName(), table.getName(), "table", table.getDescription()}; }).collect(Collectors.toList())
                    ).build();
            // TODO 显示约束信息
            //"Constraint:\n" +
            //        "    " + primaryKey.toPrettyString() + "\n" +
            //        constraint.stream().map(Constraint::toPrettyString).map(s -> "    " + s).collect(
            //                Collectors.joining("\n"))
        } else if ("SEARCH_PATH".equals(sqlSuffix)) {
            checkSchema();
            return builder.schema(schema.getName()).build();
        } else {
            throw new UnsupportedOperationException("not supported: " + sql);
        }
    }
}
