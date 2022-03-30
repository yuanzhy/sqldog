package com.yuanzhy.sqldog.server.storage.builder;

import com.yuanzhy.sqldog.server.core.Constraint;
import com.yuanzhy.sqldog.server.core.constant.ConstraintType;
import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.server.storage.memory.MemoryConstraint;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 * @author yuanzhy
 * @date 2021-10-27
 */
public class ConstraintBuilder extends BaseBuilder<ConstraintBuilder> {

    /** 类型 */
    private ConstraintType type;
    /** 列名 */
    private List<String> columnNames = new ArrayList<>();

    public ConstraintBuilder type(ConstraintType type) {
        this.type = type;
        return this;
    }

    public ConstraintBuilder addColumnName(String columnName) {
        this.columnNames.add(columnName.toUpperCase());
        return this;
    }

    @Override
    protected ConstraintBuilder getSelf() {
        return this;
    }

    @Override
    public Constraint build() {
        Asserts.notNull(type, "约束类型不能为空");
        if (StringUtils.isEmpty(this.name)) {
            this.name = type.name() + "_" + columnNames.stream().map(String::toUpperCase).collect(
                    Collectors.joining("_"));
        }
        return new MemoryConstraint(name, type, columnNames.toArray(new String[0]));
    }
}
