package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.core.util.StringUtils;
import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.sql.result.SqlResultBuilder;
import com.yuanzhy.sqldog.server.util.Databases;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class DropSchemaCommand extends AbstractSqlCommand {
    public DropSchemaCommand(String sql) {
        super(sql);
    }

    @Override
    public SqlResult execute() {
        // drop schema SCHEMA_NAME
        String schemaName = sql.substring("DROP SCHEMA ".length());
        schemaName = StringUtils.substringBefore(schemaName, " ");
        Schema schema = Databases.getDefault().getSchema(schemaName);
        if (schema == null) {
            throw new IllegalArgumentException(schemaName + " not exists");
        }
        Databases.getDefault().dropSchema(schemaName);
        return new SqlResultBuilder(StatementType.DDL).schema(schemaName).build();
    }
}
