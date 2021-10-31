package com.yuanzhy.sqldog.core;

import java.util.List;
import java.util.Map;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public interface Table extends Base {

    Map<String, Column> getColumn();

    List<Object[]> getData();

    DML getDML();

    void addColumn(Column column);

    void dropColumn(String columnName);

    void truncate();

//    Query getQuery();

}
