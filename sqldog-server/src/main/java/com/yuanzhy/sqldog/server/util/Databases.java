package com.yuanzhy.sqldog.server.util;

import com.yuanzhy.sqldog.server.core.Database;
import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.core.util.Asserts;
import com.yuanzhy.sqldog.server.memory.DatabaseBuilder;
import org.apache.commons.lang3.StringUtils;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class Databases {
    // TODO 默认先只支持单实例库
    private static final Database DEFAULT = new DatabaseBuilder().name("default").description("sqldog default db").build();

    private static final ThreadLocal<String> TL = new ThreadLocal<>();

    public static Database getDefault() {
        return DEFAULT;
    }

    public static Schema currSchema() {
        String schemaName = TL.get();
        Asserts.hasText(schemaName, "current schema is null, please execute 'use schema_name'");
        Schema schema = getDefault().getSchema(schemaName);
        Asserts.notNull(schema, "current schema '" + schemaName + "' not exists");
        return schema;
    }

    public static void currSchema(String schemaName) {
        if (StringUtils.isEmpty(schemaName)) {
            TL.remove();
        } else {
            Schema schema = getDefault().getSchema(schemaName);
            Asserts.notNull(schema, "current schema '" + schemaName + "' not exists");
            TL.set(schemaName);
        }
    }
}