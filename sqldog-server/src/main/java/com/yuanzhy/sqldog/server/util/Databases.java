package com.yuanzhy.sqldog.server.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.core.Database;
import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.sql.decorator.DatabaseDecorator;
import com.yuanzhy.sqldog.server.storage.builder.DatabaseBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class Databases {
    // TODO 默认先只支持单实例库
    private static final Map<String, Database> DATABASES = new HashMap<>();
    private static final ThreadLocal<String> TL = new ThreadLocal<>();

    static {
        final String DEF_NAME = StorageConst.DEF_DATABASE_NAME;
        if (ConfigUtil.isDisk()) {
            try {
                loadDbFromDisk();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            DATABASES.put(DEF_NAME, new DatabaseDecorator(new DatabaseBuilder().name(DEF_NAME).description("sqldog default db").build()));
        }
    }

    private static void loadDbFromDisk() throws IOException {
        String dataPath = ConfigUtil.getDataPath();
        File dataFolder = new File(dataPath);
        File[] dbFolders = dataFolder.listFiles(pathname -> pathname.isDirectory());
        if (ArrayUtils.isEmpty(dbFolders)) {
            // 创建一个default库和PUBLIC schema
            File defaultFolder = new File(dataFolder, StorageConst.DEF_DATABASE_NAME);
            File defaultMeta = new File(defaultFolder, StorageConst.META);
            final String defMeta = "{\"name\": \"default\",\"encoding\": \"UTF-8\",\"description\": \"sqldog default db\"}";
            FileUtils.writeStringToFile(defaultMeta, defMeta, "UTF-8");
            File publicFolder = new File(defaultFolder, StorageConst.DEF_SCHEMA_NAME);
            File publicMeta = new File(publicFolder, StorageConst.META);
            final String pubMeta = "{\"name\": \"PUBLIC\",\"description\": \"The default schema\"}";
            FileUtils.writeStringToFile(publicMeta, pubMeta, "UTF-8");
            Database db = new DatabaseDecorator(new DatabaseBuilder().name(StorageConst.DEF_DATABASE_NAME).description("sqldog default db").build());
            DATABASES.put(StorageConst.DEF_DATABASE_NAME, db);
        } else {
            for (File dbFolder : dbFolders) {
                File dbMetaFile = new File(dbFolder, StorageConst.META);
                if (dbMetaFile.exists()) {
                    String name = dbMetaFile.getName();
                    JSONObject dbMeta = JSON.parseObject(FileUtils.readFileToString(dbMetaFile, "UTF-8"));
                    Database db = new DatabaseDecorator(
                            new DatabaseBuilder().name(name).encoding(dbMeta.getString("encoding"))
                                    .tablespace(dbMeta.getString("tablespace")).description(dbMeta.getString("description")).build());
                    DATABASES.put(name, db);
                }
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
