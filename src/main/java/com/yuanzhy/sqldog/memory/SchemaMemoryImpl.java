package com.yuanzhy.sqldog.memory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.yuanzhy.sqldog.core.Schema;
import com.yuanzhy.sqldog.core.Table;

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

    SchemaMemoryImpl(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Set<String> getTableNames() {
        return Collections.unmodifiableSet(tables.keySet());
    }

    @Override
    public Table getTable(String name) {
        return tables.get(name);
    }
    @Override
    public void addTable(Table table) {
        this.tables.put(table.getName(), table);
    }
}
