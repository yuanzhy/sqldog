package com.yuanzhy.sqldog.memory;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.yuanzhy.sqldog.builder.BaseBuilder;
import com.yuanzhy.sqldog.core.Column;
import com.yuanzhy.sqldog.core.Constraint;
import com.yuanzhy.sqldog.core.Serial;
import com.yuanzhy.sqldog.core.Table;
import com.yuanzhy.sqldog.core.constant.ConstraintType;
import com.yuanzhy.sqldog.util.Asserts;

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
        Asserts.hasText(name, "表名称不能为空");
        Asserts.notNull(primaryKey, "主键不能为空");
        Asserts.hasEle(columnMap, "列不能为空");
        Column pkColumn = columnMap.get(primaryKey.getColumnNames()[0]);
        Asserts.notNull(pkColumn, "主键列不存在");
        // setSerial
        if (this.serial == null && pkColumn.getDataType().isSerial()) {
            this.serial = new SerialMemoryImpl(0, 1); // 默认步长
        }
        return new TableMemoryImpl(name, columnMap, primaryKey, constraint, serial);
    }
}
