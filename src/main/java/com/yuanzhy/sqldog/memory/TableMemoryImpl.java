package com.yuanzhy.sqldog.memory;

import com.yuanzhy.sqldog.core.Column;
import com.yuanzhy.sqldog.core.Constraint;
import com.yuanzhy.sqldog.core.Serial;
import com.yuanzhy.sqldog.core.Table;
import com.yuanzhy.sqldog.core.constant.ConstraintType;
import com.yuanzhy.sqldog.util.Asserts;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public class TableMemoryImpl implements Table {
    /** 名称 */
    private final String name;
    /** 列 */
    private final Map<String, Column> columnMap;
    /** 约束 */
    private final Constraint primaryKey;
    private final Serial serial;
    private final Set<Constraint> constraint;

    /** 数据 */
    private final Map<Object, Map<String, Object>> data = new ConcurrentHashMap<>();

    /** 索引 */
//    private final List<>

    private TableMemoryImpl(String name, Map<String, Column> columnMap, Constraint primaryKey, Set<Constraint> constraint, Serial serial) {
        this.name = name;
        this.columnMap = columnMap;
        this.primaryKey = primaryKey;
        this.constraint = constraint;
        this.serial = serial;
    }

    @Override
    public String getName() {
        return this.name;
    }

    private String getPkName() {
        return primaryKey.getColumnNames()[0]; // TODO 暂不支持联合主键
    }

    @Override
    public Object insert(Map<String, Object> values) {
        // check nullable
        for (Column column : columnMap.values()) {
            if (column.isNullable()) {
                if (!values.containsKey(column.getName())) {
                    values.put(column.getName(), null); // 放置空值
                }
            } else if (values.get(column.getName()) == null) {
                throw new IllegalArgumentException("'" + column.getName() + "' is not null");
            }

        }
        // generate pk
        Object pkValue;
        String pkName = getPkName();
        if (values.containsKey(pkName)) {
            pkValue = values.get(pkName);
            // check pk
            Asserts.isTrue(data.containsKey(pkName), "主键值冲突：" + pkValue);
        } else if (columnMap.get(pkName).getDataType().isSerial()) {
            pkValue = this.serial.next();
            values.put(pkName, pkValue);
        } else {
            throw new IllegalArgumentException("主键值不能为空");
        }
        this.data.put(pkValue, values);
        return pkValue;
    }

    @Override
    public int delete(Object id) {
        Object removeValue = data.remove(id);
        return removeValue == null ? 0 : 1;
    }

    @Override
    public int update(Map<String, Object> updates, Object id) {
        Map<String, Object> row = data.get(id);
        if (row == null) {
            return 0;
        }
        row.putAll(updates);
        return 1;
    }

    @Override
    public synchronized int deleteBy(Map<String, Object> wheres) {
        Iterator<Map.Entry<Object, Map<String, Object>>> ite = data.entrySet().iterator();
        int count = 0;
        while (ite.hasNext()) {
            Map.Entry<Object, Map<String, Object>> entry = ite.next();
            Map<String, Object> row = entry.getValue();
            if (isMatch(row, wheres)) {
                ite.remove();
                count++;
            }
        }
        return count;
    }

    @Override
    public synchronized int updateBy(Map<String, Object> updates, Map<String, Object> wheres) {
        int count = 0;
        for (Map.Entry<Object, Map<String, Object>> rowEntry : data.entrySet()) {
            Map<String, Object> row = rowEntry.getValue();
            if (isMatch(row, wheres)) {
                row.putAll(updates);
                count++;
            }
        }
        return count;
    }

    private boolean isMatch(Map<String, Object> row, Map<String, Object> wheres) {
        for (Map.Entry<String, Object> whereEntry : wheres.entrySet()) {
            Object whereValue = whereEntry.getValue();
            Object whereKey = whereEntry.getKey();
            if (!Objects.equals(whereValue, row.get(whereKey))) {
                return false;
            }
        }
        return true;
    }

    public class MemoryTableBuilder {
        private String name;
        private Constraint primaryKey;
        private final Map<String, Column> columnMap = new LinkedHashMap<>();
        private final Set<Constraint> constraint = new HashSet<>();
        private Serial serial;
        public MemoryTableBuilder name(String name) {
            this.name = name;
            return this;
        }
        public MemoryTableBuilder addColumn(Column column) {
            this.columnMap.put(column.getName(), column);
            return this;
        }
        public MemoryTableBuilder addConstraint(Constraint constraint) {
            if (constraint.getType() == ConstraintType.PRIMARY_KEY) {
                this.primaryKey = constraint;
            } else {
                this.constraint.add(constraint);
            }
            return this;
        }

        public MemoryTableBuilder serial(long initValue, int step) {
            this.serial = new SerialMemoryImpl(0, 1);
            return this;
        }

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
}
