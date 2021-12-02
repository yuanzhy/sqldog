package com.yuanzhy.sqldog.server.sql.result;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.ColumnMetaData;
import com.yuanzhy.sqldog.core.sql.ParamMetaData;
import com.yuanzhy.sqldog.core.sql.ParamMetaDataImpl;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.core.sql.SqlResultImpl;
import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.server.core.Constraint;

import java.sql.ParameterMetaData;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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
    /** 参数元数据 */
    private ParamMetaData[] params;
    /** 参数元数据 未处理的 */
    private ParameterMetaData pmd;
    /** 列元数据 */
    private ColumnMetaData[] columns;
    /** 列元数据 未处理的 */
    private ResultSetMetaData rsmd;
    /** 数据 */
    private List<Object[]> data;
    /** 约束信息 */
    private List<Constraint> constraintList;

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

    public SqlResultBuilder columns(ResultSetMetaData rsmd) {
        this.rsmd = rsmd;
        return this;
    }

    public SqlResultBuilder params(ParamMetaData... params) {
        this.params = params;
        return this;
    }

    public SqlResultBuilder params(ParameterMetaData pmd) {
        this.pmd = pmd;
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

    public SqlResultBuilder constraints(List<Constraint> constraintList) {
        this.constraintList = constraintList;
        return this;
    }

    public SqlResult build() {
        if (this.columns == null) {
            if (this.rsmd != null) {
                try {
                    columns = new ColumnMetaData[rsmd.getColumnCount()];
                    for (int i = 0; i < rsmd.getColumnCount(); i++) {
                        final int index = i + 1;
                        columns[i] = new ColumnMetaDataBuilder()
                                .columnClassName(rsmd.getColumnClassName(index))
                                .columnName(rsmd.getColumnName(index))
                                .autoIncrement(rsmd.isAutoIncrement(index))
                                .caseSensitive(rsmd.isCaseSensitive(index))
                                .catalogName(rsmd.getCatalogName(index))
                                .currency(rsmd.isCurrency(index))
                                .definitelyWritable(rsmd.isDefinitelyWritable(index))
                                .displaySize(rsmd.getColumnDisplaySize(index))
                                .searchable(rsmd.isSearchable(index))
                                .nullable(rsmd.isNullable(index))
                                .precision(rsmd.getPrecision(index))
                                .scale(rsmd.getScale(index))
                                .signed(rsmd.isSigned(index))
                                .label(rsmd.getColumnLabel(index))
                                .ordinal(i)
                                .readOnly(rsmd.isReadOnly(index))
                                .schemaName(rsmd.getSchemaName(index))
                                .tableName(rsmd.getTableName(index))
                                .writable(rsmd.isWritable(index))
                                .build();
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            } else if (this.labels != null) {
                this.columns = new ColumnMetaData[this.labels.length];
                ColumnMetaDataBuilder columnBuilder = new ColumnMetaDataBuilder();
                for (int i = 0; i < this.labels.length; i++) {
                    String label = this.labels[i];
                    this.columns[i] = columnBuilder.ordinal(i).label(label).columnName(label).columnClassName(String.class.getName()).build();
                }
            }
        }
        if (this.params == null && this.pmd != null) {
            try {
                params = new ParamMetaData[pmd.getParameterCount()];
                for (int i = 0; i < pmd.getParameterCount(); i++) {
                    final int index = i + 1;
                    params[i] = new ParamMetaDataImpl(
                            pmd.isSigned(index),
                            pmd.getPrecision(index),
                            pmd.getScale(index),
                            pmd.getParameterType(index),
                            pmd.getParameterTypeName(index),
                            pmd.getParameterClassName(index),
                            pmd.getParameterMode(index),
                            pmd.isNullable(index)
                    );
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        com.yuanzhy.sqldog.core.sql.Constraint[] constraints = null;
        if (constraintList != null && !constraintList.isEmpty()) {
            constraints = constraintList.stream()
                    .map(c -> new com.yuanzhy.sqldog.core.sql.Constraint(c.getName(), c.getType().name(), c.getColumnNames()))
                    .toArray(com.yuanzhy.sqldog.core.sql.Constraint[]::new);
        }
        return new SqlResultImpl(type, rows, schema, table, params, columns, data, constraints);
    }
}
