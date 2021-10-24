package com.yuanzhy.sqldog.core;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public interface Schema extends Base {

    Table getTable(String name);

    void addTable(String name, Table table);
}
