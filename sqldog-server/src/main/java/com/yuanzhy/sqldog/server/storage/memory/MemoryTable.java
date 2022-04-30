package com.yuanzhy.sqldog.server.storage.memory;

import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.server.core.Base;
import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.core.Constraint;
import com.yuanzhy.sqldog.server.core.Serial;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.core.TableData;
import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public class MemoryTable extends MemoryBase implements Table {
    /** 列 */
    protected final Map<String, Column> columnMap;
    /** 约束 */
    protected Constraint primaryKey;
    protected Serial serial;
    protected Set<Constraint> constraint;

    /**
     * 数据
     */
    protected TableData tableData;

    protected MemoryTable(Base parent) {
        super(parent);
        columnMap = new LinkedHashMap<>();
        constraint = new LinkedHashSet<>();
        this.initTableData();
    }

    public MemoryTable(Base parent, String name, Map<String, Column> columnMap, Constraint primaryKey, Set<Constraint> constraint, Serial serial) {
        super(parent, name.toUpperCase());
        this.columnMap = columnMap;
        this.primaryKey = primaryKey;
        this.constraint = constraint;
        this.serial = serial;
        this.initTableData();
    }

    protected void initTableData() {
        this.tableData = new MemoryTableData(this);
    }

    @Override
    public void drop() {
        this.columnMap.clear();
        this.constraint.clear();
//        this.uniqueMap.clear();
        this.getTableData().truncate();
    }

    @Override
    public String[] getPkColumnName() {
        return primaryKey == null ? null : primaryKey.getColumnNames();
    }

    @Override
    public Constraint getPrimaryKey() {
        return primaryKey;
    }

    @Override
    public Serial getSerial() {
        return serial;
    }

    @Override
    public List<Constraint> getConstraints() {
        List<Constraint> r = new ArrayList<>();
        if (primaryKey != null) {
            r.add(primaryKey);
        }
        if (constraint != null) {
            r.addAll(constraint);
        }
        return r.isEmpty() ? Collections.emptyList() : r;
    }

    @Override
    public Column getColumn(String name) {
        return this.columnMap.get(name);
    }

    @Override
    public Map<String, Column> getColumns() {
        return Collections.unmodifiableMap(this.columnMap);
    }

    @Override
    public TableData getTableData() {
        return tableData;
    }

    @Override
    public synchronized void addColumn(Column column) {
        if (this.columnMap.containsKey(column.getName())) {
            throw new IllegalArgumentException(column.getName() + " exists");
        }
        this.columnMap.put(column.getName(), column);
        getTableData().addColumn(column);
    }

    @Override
    public synchronized void dropColumn(String columnName) {
        int deleteIndex = ArrayUtils.indexOf(this.columnMap.keySet().toArray(), columnName);
        Column column = this.columnMap.remove(columnName);
        getTableData().dropColumn(column, deleteIndex);
    }

    @Override
    public void updateColumnDescription(String colName, String description) {
        Column column = this.getColumn(colName);
        Asserts.notNull(column, colName + " not exists");
        column.setDescription(description);
    }
}
