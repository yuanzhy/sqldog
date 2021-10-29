package com.yuanzhy.sqldog.memory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.yuanzhy.sqldog.core.Constraint;
import com.yuanzhy.sqldog.core.constant.ConstraintType;
import com.yuanzhy.sqldog.util.Asserts;

/**
 *
 * @author yuanzhy
 * @date 2021-10-27
 */
public class ConstraintBuilder {

    /** 名称 */
    private String name;
    /** 类型 */
    private ConstraintType type;
    /** 列名 */
    private List<String> columnNames = new ArrayList<>();

    public ConstraintBuilder name(String name) {
        this.name = name;
        return this;
    }

    public ConstraintBuilder type(ConstraintType type) {
        this.type = type;
        return this;
    }

    public ConstraintBuilder addColumnName(String columnName) {
        this.columnNames.add(columnName);
        return this;
    }

    public Constraint build() {
        Asserts.notNull(type, "约束类型不能为空");
        if (StringUtils.isEmpty(this.name)) {
            this.name = type.name().concat(columnNames.stream().map(String::toUpperCase).collect(
                    Collectors.joining("_")));
        }
        return new ConstraintMemoryImpl(name, type, columnNames.toArray(new String[0]));
    }
}
