package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.sql.SqlResultBuilder;
import com.yuanzhy.sqldog.server.util.Databases;

import java.util.Set;

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
            return builder.headers("Database", "Name", "Description")
                    .data(Databases.getDefault().getSchemaNames().stream().map(s -> {
                        Schema schema = Databases.getDefault().getSchema(s);
                        return new Object[]{dbName, schema.getName(), schema.getDescription()};
                    })).build();
        } else if ("TABLES".equals(sqlSuffix)) {
            Schema schema = Databases.currSchema();
            return builder.headers("Schema", "Name", "Type", "Description")
                    .schema(schema.getName())
                    .data(schema.getTableNames().stream().map(t -> {
                        Table table = schema.getTable(t);
                        return new Object[]{schema.getName(), table.getName(), "table", schema.getDescription()};
                    })).build();
        } else if ("SEARCH_PATH".equals(sqlSuffix)) {
            return builder.schema(Databases.currSchema().getName()).build();
        } else {
            throw new UnsupportedOperationException("not supported: " + sql);
        }
    }
}
