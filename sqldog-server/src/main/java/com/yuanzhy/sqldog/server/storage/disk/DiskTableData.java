package com.yuanzhy.sqldog.server.storage.disk;

import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.core.TableData;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlUpdate;

import java.util.List;
import java.util.Map;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/4
 */
public class DiskTableData implements TableData {

    private final Table table;
    DiskTableData(Table table) {
        this.table = table;
    }
    @Override
    public Object[] insert(Map<String, Object> values) {
        return new Object[0];
    }

    @Override
    public int deleteBy(SqlDelete sqlDelete) {
        return 0;
    }

    @Override
    public int updateBy(SqlUpdate sqlUpdate) {
        return 0;
    }

    @Override
    public void truncate() {

    }

    @Override
    public List<Object[]> getData() {
        return null;
    }

    @Override
    public void addColumn(Column column) {

    }

    @Override
    public void dropColumn(String columnName) {

    }
}
