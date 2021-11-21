package com.yuanzhy.sqldog.core.sql;

import com.yuanzhy.sqldog.core.constant.StatementType;

import java.util.Arrays;
import java.util.List;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/17
 */
public class SqlResultImpl implements SqlResult {

    private static final long serialVersionUID = 1L;
    /** sql分类 */
    private final StatementType type;
    /** 影响行数 */
    private final long rows;
    /** 模式名 */
    private final String schema;
    /** 表名 */
    private final String table;
    /** 列信息 */
    private final ColumnMetaData[] columns;
    /** sql分类 */
    private final List<Object[]> data;

    public SqlResultImpl(StatementType type, long rows, String schema, String table, ColumnMetaData[] columns, List<Object[]> data) {
        this.type = type;
        this.rows = rows;
        this.schema = schema;
        this.table = table;
        this.columns = columns;
        this.data = data;
    }

    @Override
    public StatementType getType() {
        return type;
    }

    @Override
    public long getRows() {
        return rows;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public String getTable() {
        return table;
    }

    @Override
    public String[] getLabels() {
        if (columns == null) {
            return null;
        }
        return Arrays.stream(columns).map(ColumnMetaData::getLabel).toArray(String[]::new);
    }

    @Override
    public ColumnMetaData[] getColumns() {
        return columns;
    }

    @Override
    public List<Object[]> getData() {
        return data;
    }
}
