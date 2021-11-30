package com.yuanzhy.sqldog.server.memory;

import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.core.Constraint;
import com.yuanzhy.sqldog.server.core.Serial;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.core.builder.BaseBuilder;
import com.yuanzhy.sqldog.server.core.constant.ConstraintType;
import com.yuanzhy.sqldog.core.util.Asserts;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author yuanzhy
 * @date 2021-10-26
 */
public class TableBuilder extends BaseBuilder<TableBuilder> {
    private Constraint primaryKey;
    private final Map<String, Column> columnMap = new LinkedHashMap<>();
    private final Set<Constraint> constraint = new HashSet<>();
    private Serial serial;

    public TableBuilder addColumn(Column column) {
        this.columnMap.put(column.getName(), column);
        return this;
    }
    public TableBuilder addConstraint(Constraint constraint) {
        if (constraint.getType() == ConstraintType.PRIMARY_KEY) {
            this.primaryKey = constraint;
        } else {
            this.constraint.add(constraint);
        }
        return this;
    }

    public TableBuilder serial(long initValue, int step) {
        this.serial = new SerialMemoryImpl(0, 1);
        return this;
    }

    @Override
    protected TableBuilder getSelf() {
        return this;
    }

    @Override
    public Table build() {
        Asserts.hasText(name, "table name must not be null");
        //Asserts.notNull(primaryKey, "primaryKey must not be null");
        Asserts.hasEle(columnMap, "table must has column");
        Column pkColumn = columnMap.get(primaryKey.getColumnNames()[0]);
        Asserts.notNull(pkColumn, "primaryKey column '"+primaryKey.getColumnNames()[0]+"' not exists");
        // setSerial
        if (this.serial == null && pkColumn.getDataType().isSerial()) {
            this.serial = new SerialMemoryImpl(0, 1); // 默认步长
        }
        return new TableMemoryImpl(name, columnMap, primaryKey, constraint, serial);
    }
}
