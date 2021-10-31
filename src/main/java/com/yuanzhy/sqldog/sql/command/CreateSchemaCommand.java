package com.yuanzhy.sqldog.sql.command;

import com.yuanzhy.sqldog.core.Schema;
import com.yuanzhy.sqldog.memory.SchemaBuilder;
import com.yuanzhy.sqldog.util.Databases;
import org.apache.commons.lang3.StringUtils;

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
    public void execute() {
        // create schema SCHEMA_NAME
        String schemaName = sql.substring("CREATE SCHEMA ".length());
        schemaName = StringUtils.substringBefore(schemaName, " ");
        Schema schema = new SchemaBuilder().name(schemaName).build();
        Databases.getDefault().addSchema(schema);
    }
}
