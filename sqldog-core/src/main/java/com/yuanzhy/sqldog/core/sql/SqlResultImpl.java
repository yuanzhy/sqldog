package com.yuanzhy.sqldog.core.sql;

import com.yuanzhy.sqldog.core.constant.StatementType;

import java.io.Serializable;
import java.util.List;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/17
 */
public class SqlResultImpl implements SqlResult, Serializable {
    /** sql分类 */
    private final StatementType type;
    /** 影响行数 */
    private final int rows;
    /** 模式名 */
    private final String schema;
    /** 表名 */
    private final String table;
    /** sql分类 */
    private final String[] headers;
    /** sql分类 */
    private final List<Object[]> data;

    public SqlResultImpl(StatementType type, int rows, String schema, String table, String[] headers, List<Object[]> data) {
        this.type = type;
        this.rows = rows;
        this.schema = schema;
        this.table = table;
        this.headers = headers;
        this.data = data;
    }

    @Override
    public StatementType getType() {
        return type;
    }

    @Override
    public int getRows() {
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
    public String[] getHeaders() {
        return headers;
    }

    @Override
    public List<Object[]> getData() {
        return data;
    }
}
