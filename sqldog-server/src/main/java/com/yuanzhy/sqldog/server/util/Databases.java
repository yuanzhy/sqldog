package com.yuanzhy.sqldog.server.util;

import com.yuanzhy.sqldog.server.memory.SchemaBuilder;
import org.apache.commons.lang3.StringUtils;

import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.server.core.Database;
import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.memory.DatabaseBuilder;
import com.yuanzhy.sqldog.server.sql.decorator.DatabaseDecorator;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class Databases {
    // TODO 默认先只支持单实例库
    private static final Database DEFAULT = new DatabaseDecorator(new DatabaseBuilder().name("default").description("sqldog default db").build());
    private static final ThreadLocal<String> TL = new ThreadLocal<>();

    public static final String DEFAULT_SCHEMA = "PUBLIC";

    static {
        DEFAULT.addSchema(new SchemaBuilder().name(DEFAULT_SCHEMA).description("The default schema").build());
    }

    public static Database getDefault() {
        return DEFAULT;
    }

    public static String currSchema() {
        return TL.get();
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
