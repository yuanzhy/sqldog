package com.yuanzhy.sqldog.server.sql.result;

import java.util.Collections;
import java.util.List;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.core.sql.SqlResultImpl;
import com.yuanzhy.sqldog.core.util.Asserts;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/17
 */
public class SqlResultBuilder {

    /** sql分类 */
    private final StatementType type;
    /** 影响行数 */
    private int rows = 0;
    /** 模式名 */
    private String schema;
    /** 表名 */
    private String table;
    /** 表头 */
    private String[] headers;
    /** 数据 */
    private List<Object[]> data;

    public SqlResultBuilder(StatementType type) {
        Asserts.notNull(type, "StatementType can not be null");
        this.type = type;
    }

//    public SqlResultBuilder type(StatementType type) {
//        this.type = type;
//        return this;
//    }

    public SqlResultBuilder rows(int rows) {
        Asserts.gteZero(rows, "rows must great than or equal zero");
        this.rows = rows;
        return this;
    }

    public SqlResultBuilder schema(String schema) {
        this.schema = schema;
        return this;
    }

    public SqlResultBuilder table(String table) {
        this.table = table;
        return this;
    }

    public SqlResultBuilder headers(String... headers) {
        this.headers = headers;
        return this;
    }

    public SqlResultBuilder data(List<Object[]> data) {
        this.data = data;
        return this;
    }

    public SqlResultBuilder data(Object data) {
        this.data = Collections.singletonList(new Object[]{data});
        return this;
    }

    public SqlResult build() {
        return new SqlResultImpl(type, rows, schema, table, headers, data);
    }
}
