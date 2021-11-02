package com.yuanzhy.sqldog.memory;

import com.yuanzhy.sqldog.core.Database;
import com.yuanzhy.sqldog.core.Schema;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public class DatabaseMemoryImpl extends MemoryBase implements Database {
    /** 编码 */
    private final String encoding;
    /** 表空间 */
    private final String tablespace;
    /** 模式 */
    private final Map<String, Schema> schemas = new HashMap<>();

    DatabaseMemoryImpl(String name, String encoding, String description, String tablespace) {
        super(name.toUpperCase());
        this.encoding = encoding;
        this.description = description;
        this.tablespace = tablespace;
    }

    @Override
    public void drop() {
        this.schemas.forEach((k, v) -> v.drop());
        this.schemas.clear();
    }

    @Override
    public String getEncoding() {
        return encoding;
    }

    @Override
    public String getTablespace() {
        return tablespace;
    }
    @Override
    public Schema getSchema(String name) {
        return schemas.get(name.toUpperCase());
    }
    @Override
    public void addSchema(Schema schema) {
        if (this.schemas.containsKey(schema.getName())) {
            throw new IllegalArgumentException(schema.getName() + " exists");
        }
        this.schemas.put(schema.getName(), schema);
    }

    @Override
    public void dropSchema(String schemaName) {
        Schema schema = this.schemas.remove(schemaName.toUpperCase());
        if (schema != null) {
            schema.drop();
        }
    }
}
