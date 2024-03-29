package com.yuanzhy.sqldog.server.storage.memory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.yuanzhy.sqldog.server.core.Base;
import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.core.Table;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public class MemorySchema extends MemoryBase implements Schema {
    /** 表 */
    private transient final Map<String, Table> tables = new LinkedHashMap<>();

    protected MemorySchema(Base parent) {
        super(parent);
    }

    public MemorySchema(Base parent, String name, String description) {
        super(parent, name.toUpperCase());
        this.description = description;
    }

    @Override
    public void drop() {
        this.tables.forEach((k, v) -> v.drop());
        this.tables.clear();
        this.deleteObservers();
    }

    @Override
    public Set<String> getTableNames() {
        return Collections.unmodifiableSet(tables.keySet());
    }

    @Override
    public Table getTable(String name) {
        return tables.get(name.toUpperCase());
    }
    @Override
    public void addTable(Table table) {
        if (this.tables.containsKey(table.getName())) {
            throw new IllegalArgumentException(table.getName() + " exists");
        }
        this.tables.put(table.getName(), table);
        this.setChanged();
        this.notifyObservers();
    }

    @Override
    public void dropTable(String name) {
        Table table = this.tables.remove(name);
        if (table != null) {
            this.setChanged();
            this.notifyObservers();
            table.drop();
        }
    }

    @Override
    public void renameTable(String oldName, String newName) {
        Table table = this.tables.remove(oldName);
        if (table != null) {
            this.tables.put(newName, table);
            this.setChanged();
            this.notifyObservers();
        }
    }
}
