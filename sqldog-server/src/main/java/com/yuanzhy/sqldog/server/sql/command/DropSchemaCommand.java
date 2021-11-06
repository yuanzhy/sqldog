package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.util.Databases;
import org.apache.commons.lang3.StringUtils;

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
    public String execute() {
        // drop schema SCHEMA_NAME
        String schemaName = sql.substring("DROP SCHEMA ".length());
        schemaName = StringUtils.substringBefore(schemaName, " ");
        Schema schema = Databases.getDefault().getSchema(schemaName);
        if (schema == null) {
            throw new IllegalArgumentException(schemaName + " not exists");
        }
        Databases.getDefault().dropSchema(schemaName);
        return success();
    }
}
