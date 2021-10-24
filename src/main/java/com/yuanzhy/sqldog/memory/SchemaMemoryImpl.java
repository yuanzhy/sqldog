package com.yuanzhy.sqldog.memory;

import com.yuanzhy.sqldog.core.Schema;
import com.yuanzhy.sqldog.core.Table;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public class SchemaMemoryImpl implements Schema {
    /** 名称 */
    private final String name;
    /** 表 */
    private final Map<String, Table> tables = new HashMap<>();

    public SchemaMemoryImpl(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }
    @Override
    public Table getTable(String name) {
        return tables.get(name);
    }
    @Override
    public void addTable(String name, Table table) {
        this.tables.put(name, table);
    }
}
