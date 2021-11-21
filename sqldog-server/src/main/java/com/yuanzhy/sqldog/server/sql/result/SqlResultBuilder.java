package com.yuanzhy.sqldog.server.sql.result;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.ColumnMetaData;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.core.sql.SqlResultImpl;
import com.yuanzhy.sqldog.core.util.Asserts;

import java.util.Collections;
import java.util.List;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/17
 */
public class SqlResultBuilder {

    /** sql分类 */
    private final StatementType type;
    /** 影响行数 */
    private long rows = 0;
    /** 模式名 */
    private String schema;
    /** 表名 */
    private String table;
    /** 表头 */
    private String[] labels;
    /** 列元数据 */
    private ColumnMetaData[] columns;
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

    public SqlResultBuilder rows(long rows) {
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

    public SqlResultBuilder labels(String... labels) {
        this.labels = labels;
        return this;
    }

    public SqlResultBuilder columns(ColumnMetaData... columns) {
        this.columns = columns;
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
        if (this.columns == null && this.labels != null) {
            this.columns = new ColumnMetaData[this.labels.length];
            ColumnMetaDataBuilder columnBuilder = new ColumnMetaDataBuilder();
            for (int i = 0; i < this.labels.length; i++) {
                String label = this.labels[i];
                this.columns[i] = columnBuilder.ordinal(i).label(label).columnName(label).build();
            }
        }
        return new SqlResultImpl(type, rows, schema, table, columns, data);
    }
}
