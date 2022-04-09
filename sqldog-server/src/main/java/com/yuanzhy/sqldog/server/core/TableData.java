package com.yuanzhy.sqldog.server.core;

import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlUpdate;

import java.util.List;
import java.util.Map;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/4
 */
public interface TableData {

    Object[] insert(Map<String, Object> values);

//    int delete(Object id);

//    int update(Map<String, Object> updates, Object id);

    int deleteBy(SqlDelete sqlDelete);

    int updateBy(SqlUpdate sqlUpdate);

    void truncate();

    @Deprecated
    List<Object[]> getData();

    void addColumn(Column column);

    void dropColumn(Column column, int deleteIndex);
}
