package com.yuanzhy.sqldog.server.util;

import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.core.Database;
import com.yuanzhy.sqldog.server.core.Persistence;
import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.sql.decorator.DatabaseDecorator;
import com.yuanzhy.sqldog.server.storage.builder.DatabaseBuilder;
import com.yuanzhy.sqldog.server.storage.builder.SchemaBuilder;
import com.yuanzhy.sqldog.server.storage.disk.DiskDatabase;
import com.yuanzhy.sqldog.server.storage.persistence.PersistenceFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class Databases {
    // TODO 默认先只支持单实例库
    private static final Map<String, Database> DATABASES = new HashMap<>();

    static {
        if (ConfigUtil.isDisk()) {
            loadDbFromDisk();
        } else {
            // 创建一个default库和PUBLIC schema
            Database db = new DatabaseDecorator(new DatabaseBuilder().name(StorageConst.DEF_DATABASE_NAME).description("sqldog default db").build());
            Schema schema = new SchemaBuilder().name(StorageConst.DEF_SCHEMA_NAME).description("The default schema").parent(db).build();
            db.addSchema(schema);
            DATABASES.put(db.getName(), db);
        }
    }

    private static void loadDbFromDisk() {
        Persistence persistence = PersistenceFactory.get();
        List<String> dbPaths = persistence.list("");
        if (dbPaths.isEmpty()) {
            // 创建一个default库和PUBLIC schema
            Database db = new DatabaseDecorator(new DatabaseBuilder().name(StorageConst.DEF_DATABASE_NAME).description("sqldog default db").build());
            Schema schema = new SchemaBuilder().name(StorageConst.DEF_SCHEMA_NAME).description("The default schema").parent(db).build();
            db.addSchema(schema);
            DATABASES.put(db.getName(), db);
        } else {
            for (String dbPath : dbPaths) {
                Database db = new DatabaseDecorator(new DiskDatabase(dbPath));
                DATABASES.put(db.getName(), db);
            }
        }
    }

    @Deprecated
    public static Database getDefault() {
        return getDatabase(StorageConst.DEF_DATABASE_NAME);
    }

    public static Database getDatabase(String name) {
        return DATABASES.get(name);
    }

}
