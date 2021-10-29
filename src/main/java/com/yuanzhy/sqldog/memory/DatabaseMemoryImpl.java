package com.yuanzhy.sqldog.memory;

import java.util.HashMap;
import java.util.Map;

import com.yuanzhy.sqldog.core.Database;
import com.yuanzhy.sqldog.core.Schema;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public class DatabaseMemoryImpl implements Database {
    /** 名称 */
    private final String name;
    /** 编码 */
    private final String encoding;
    /** 描述 */
    private final String description;
    /** 表空间 */
    private final String tablespace;
    /** 模式 */
    private final Map<String, Schema> schemas = new HashMap<>();

    DatabaseMemoryImpl(String name, String encoding, String description, String tablespace) {
        this.name = name;
        this.encoding = encoding;
        this.description = description;
        this.tablespace = tablespace;
    }

    @Override
    public String getName() {
        return name;
    }
    @Override
    public String getEncoding() {
        return encoding;
    }
    @Override
    public String getDescription() {
        return description;
    }
    @Override
    public String getTablespace() {
        return tablespace;
    }
    @Override
    public Schema getSchema(String name) {
        return schemas.get(name);
    }
    @Override
    public void addSchema(Schema schema) {
        this.schemas.put(schema.getName(), schema);
    }
}
