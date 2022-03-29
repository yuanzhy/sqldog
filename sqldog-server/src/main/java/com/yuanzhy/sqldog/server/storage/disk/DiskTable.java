package com.yuanzhy.sqldog.server.storage.disk;

import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.core.Constraint;
import com.yuanzhy.sqldog.server.core.Serial;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.storage.memory.MemoryTable;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/3/29
 */
public class DiskTable extends MemoryTable implements Table {

    public DiskTable(String name, Map<String, Column> columnMap, Constraint primaryKey, Set<Constraint> constraint, Serial serial) {
        super(name, columnMap, primaryKey, constraint, serial);
    }

    @Override
    public String[] getPkColumnName() {
        return new String[0];
    }

    @Override
    public Constraint getPrimaryKey() {
        return null;
    }

    @Override
    public List<Constraint> getConstraints() {
        return null;
    }

    @Override
    public List<Object[]> getData() {
        return null;
    }

    @Override
    public void addColumn(Column column) {

    }

    @Override
    public void dropColumn(String columnName) {

    }

    @Override
    public void truncate() {

    }

    @Override
    public void setDescription(String description) {

    }

    @Override
    public void drop() {

    }
}
