package com.yuanzhy.sqldog.server.memory;

import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.core.Table;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public class SchemaMemoryImpl extends MemoryBase implements Schema {
    /** 表 */
    private final Map<String, Table> tables = new HashMap<>();

    SchemaMemoryImpl(String name) {
        super(name.toUpperCase());
    }
    /*
         Schema |    Name    | Type  | Description
        --------+------------+-------+----------
         public | company    | table | postgres
         public | department | table | postgres
     */
    @Override
    public String toPrettyString() {
        return "\t List of tables\n" +
                joinByVLine("Schema", "Name", "Type", "Description") + "\n" +
                genHLine(4) + "\n" +
                tables.values().stream().map(t -> joinByVLine(name, t.getName(), "table", t.getDescription())).collect(Collectors.joining("\n"))
                ;
    }

    @Override
    public void drop() {
        this.tables.forEach((k, v) -> v.drop());
        this.tables.clear();
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
    }

    @Override
    public void dropTable(String name) {
        Table table = this.tables.remove(name);
        if (table != null) {
            table.drop();
        }
    }
}
