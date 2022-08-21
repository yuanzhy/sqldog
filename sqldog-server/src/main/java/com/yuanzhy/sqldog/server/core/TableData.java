package com.yuanzhy.sqldog.server.core;

import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlUpdate;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/4
 */
public interface TableData extends Iterable<Object[]> {

    Object[] insert(Map<String, Object> values);

//    int delete(Object id);

//    int update(Map<String, Object> updates, Object id);

    int deleteBy(SqlDelete sqlDelete);

    int updateBy(SqlUpdate sqlUpdate);

    void truncate();

    @Deprecated
    List<Object[]> getData();

    int getCount();

    void addColumn(Column column);

    void dropColumn(Column column, int deleteIndex);

    /**
     * 整理表并重建索引，避免存储碎片
     */
    void optimize();
}
