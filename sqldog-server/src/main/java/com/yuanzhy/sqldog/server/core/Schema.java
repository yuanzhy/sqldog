package com.yuanzhy.sqldog.server.core;

import java.util.Set;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public interface Schema extends Base {

    Set<String> getTableNames();

    Table getTable(String name);

    void addTable(Table table);

    void dropTable(String name);

    void renameTable(String oldName, String newName);
}
