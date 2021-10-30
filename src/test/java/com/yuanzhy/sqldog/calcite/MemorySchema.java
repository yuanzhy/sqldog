package com.yuanzhy.sqldog.calcite;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

/**
 *
 * @author yuanzhy
 * @date 2021-10-29
 */
public class MemorySchema extends AbstractSchema {
    private final String name;
    private final Map<String, Table> tableMap = new HashMap<>();

    MemorySchema(String name) {
        this.name = name;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return Collections.unmodifiableMap(tableMap);
    }

    public void addTable(String name, Table table) {
        tableMap.put(name, table);
    }
}
