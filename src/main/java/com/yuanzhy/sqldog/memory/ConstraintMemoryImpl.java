package com.yuanzhy.sqldog.memory;

import java.util.Arrays;
import java.util.Objects;

import com.yuanzhy.sqldog.core.Constraint;
import com.yuanzhy.sqldog.core.constant.ConstraintType;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public class ConstraintMemoryImpl implements Constraint {
    /** 名称 */
    private final String name;
    /** 类型 */
    private final ConstraintType type;
    /** 列名 */
    private final String[] columnNames;

    ConstraintMemoryImpl(String name, ConstraintType type, String[] columnNames) {
        this.name = name;
        this.type = type;
        this.columnNames = columnNames;
    }

    @Override
    public String getName() {
        return name;
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
