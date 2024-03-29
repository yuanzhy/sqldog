package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.core.util.StringUtils;
import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.sql.result.SqlResultBuilder;
import com.yuanzhy.sqldog.server.storage.builder.SchemaBuilder;
import com.yuanzhy.sqldog.server.util.Databases;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class CreateSchemaCommand extends AbstractSqlCommand {

    public CreateSchemaCommand(String sql) {
        super(sql);
    }

    @Override
    public SqlResult execute() {
        // create schema SCHEMA_NAME
        String schemaName = sql.substring("CREATE SCHEMA ".length());
        schemaName = StringUtils.substringBefore(schemaName, " ");
        Schema schema = new SchemaBuilder().parent(Databases.getDefault()).name(schemaName).build();
        Databases.getDefault().addSchema(schema);
        return new SqlResultBuilder(StatementType.DDL).schema(schemaName).build();
    }
}
