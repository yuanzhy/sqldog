package com.yuanzhy.sqldog.core.sql;

import java.io.Serializable;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/30
 */
public class Constraint implements Serializable {

    private final String name;
    private final String type;
    private final String[] columnNames;

    public Constraint(String name, String type, String[] columnNames) {
        this.name = name;
        this.type = type;
        this.columnNames = columnNames;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String[] getColumnNames() {
        return columnNames;
    }
}
