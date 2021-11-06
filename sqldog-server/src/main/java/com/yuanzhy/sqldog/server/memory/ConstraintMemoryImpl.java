package com.yuanzhy.sqldog.server.memory;

import com.yuanzhy.sqldog.server.core.Constraint;
import com.yuanzhy.sqldog.server.core.constant.ConstraintType;

import java.util.Arrays;
import java.util.Objects;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public class ConstraintMemoryImpl extends MemoryBase implements Constraint {
    /** 类型 */
    private final ConstraintType type;
    /** 列名 */
    private final String[] columnNames;

    ConstraintMemoryImpl(String name, ConstraintType type, String[] columnNames) {
        super(name.toUpperCase());
        this.type = type;
        this.columnNames = columnNames;
    }

    @Override
    public void drop() {

    }

    public ConstraintType getType() {
        return type;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConstraintMemoryImpl that = (ConstraintMemoryImpl) o;
        return Objects.equals(name, that.name) &&
                type == that.type &&
                Arrays.equals(columnNames, that.columnNames);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(name, type);
        result = 31 * result + Arrays.hashCode(columnNames);
        return result;
    }
}
