package com.yuanzhy.sqldog.core.sql;

import com.yuanzhy.sqldog.core.constant.StatementType;

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
    /** 参数信息 preparedStatement */
    private final ParamMetaData[] params;
    /** 列信息 */
    private final ColumnMetaData[] columns;
    /** sql分类 */
    private final List<Object[]> data;
    /** 约束信息 */
    private final Constraint[] constraints;

    public SqlResultImpl(StatementType type, long rows, String schema, String table, ParamMetaData[] params, ColumnMetaData[] columns, List<Object[]> data, Constraint[] constraints) {
        this.type = type;
        this.rows = rows;
        this.schema = schema;
        this.table = table;
        this.params = params;
        this.columns = columns;
        this.data = data;
        this.constraints = constraints;
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
    public ParamMetaData[] getParams() {
        return this.params;
    }

    @Override
    public ColumnMetaData[] getColumns() {
        return columns;
    }

    @Override
    public List<Object[]> getData() {
        return data;
    }

    @Override
    public Constraint[] getConstraints() {
        return constraints;
    }
}
