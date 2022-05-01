package com.yuanzhy.sqldog.server.core;

import java.util.List;
import java.util.Map;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public interface Table extends Base {

    Column getColumn(String name);

    Map<String, Column> getColumns();

    String[] getPkColumnName();
    Constraint getPrimaryKey();

    Serial getSerial();

    List<Constraint> getConstraints();

    TableData getTableData();

    void addColumn(Column column);

    void dropColumn(String columnName);

    void updateColumnDescription(String colName, String substringBetween);

    int getColumnIndex(String columnName);

}
