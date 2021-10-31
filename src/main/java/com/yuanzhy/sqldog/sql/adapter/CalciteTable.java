package com.yuanzhy.sqldog.sql.adapter;

import com.yuanzhy.sqldog.core.Column;
import com.yuanzhy.sqldog.core.Table;
import com.yuanzhy.sqldog.util.Asserts;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 文件表需要实现 org.apache.calcite.schema.FilterableTable
 *
 * @author yuanzhy
 * @date 2021-10-26
 */
public class CalciteTable extends AbstractQueryableTable implements ScannableTable/*, ModifiableTable*/ {
    private static final Logger LOG = LoggerFactory.getLogger(CalciteTable.class);
    private static final Type TYPE = Object[].class;

    private final Table table;
//    private final List<Object[]> data = new ArrayList<>();

    public CalciteTable(Table table) {
        super(TYPE);
        Asserts.hasEle(table.getColumn(), "column is empty");
        this.table = table;
    }

    @Override
    public Statistic getStatistic() {
        return super.getStatistic();
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        Map<String, Column> columnMap = table.getColumn();
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (Map.Entry<String, Column> entry : columnMap.entrySet()) {
            Column column = entry.getValue();
            switch (column.getDataType()) {
                case INT:
                case SERIAL:
                    builder.add(entry.getKey(), SqlTypeName.INTEGER);
                    break;
                case SMALLINT:
                case SMALLSERIAL:
                    builder.add(entry.getKey(), SqlTypeName.SMALLINT);
                    break;
                case BIGINT:
                case BIGSERIAL:
                    builder.add(entry.getKey(), SqlTypeName.BIGINT);
                    break;
                case NUMERIC:
                    builder.add(entry.getKey(), SqlTypeName.DECIMAL, column.getPrecision(), column.getScale());
                    break;
                case CHAR:
                    builder.add(entry.getKey(), SqlTypeName.CHAR, column.getPrecision());
                    break;
                case VARCHAR:
                    builder.add(entry.getKey(), SqlTypeName.VARCHAR, column.getPrecision());
                    break;
                case DATE:
                    builder.add(entry.getKey(), SqlTypeName.DATE);
                    break;
                case TIME:
                    builder.add(entry.getKey(), SqlTypeName.TIME);
                    break;
                case TIMESTAMP:
                    builder.add(entry.getKey(), SqlTypeName.TIMESTAMP);
                    break;
                case BOOLEAN:
                    builder.add(entry.getKey(), SqlTypeName.BOOLEAN);
                    break;
                case ARRAY:
                    builder.add(entry.getKey(), SqlTypeName.ARRAY);
                    break;
                //case JSON:
                //    builder.add(entry.getKey(), SqlTypeName.MAP));
                //    break;
                case TEXT:
                case BYTEA:
                    builder.add(entry.getKey(), SqlTypeName.BINARY);
                    break;
            }
        }
        return builder.build();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        LOG.info("scan");
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new Enumerator<Object[]>() {
                    private int index = -1;
                    private List<Object[]> data = getData();

                    @Override
                    public Object[] current() {
                        Object[] current = data.get(this.index);
                        return current;
                        //return current != null && current.getClass().isArray() ? (Object[])(current) : new Object[]{current};
                    }

                    @Override
                    public boolean moveNext() {
                        if (cancelFlag != null && cancelFlag.get()) {
                            return false;
                        } else {
                            return ++this.index < data.size();
                        }
                    }

                    @Override
                    public void reset() {
                        this.index = -1;
                    }

                    @Override
                    public void close() {

                    }
                };
            }
        };
    }

//    @Override
//    public @Nullable Collection getModifiableCollection() {
//        LOG.info("getModifiableCollection");
//        return getData();
//    }
//
//    @Override
//    public TableModify toModificationRel(RelOptCluster cluster, RelOptTable table, Prepare.CatalogReader catalogReader, RelNode child, TableModify.Operation operation, @Nullable List<String> updateColumnList, @Nullable List<RexNode> sourceExpressionList, boolean flattened) {
//        LOG.info("toModificationRel");
//        return LogicalTableModify.create(table, catalogReader, child, operation,
//                updateColumnList, sourceExpressionList, flattened);
//    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        LOG.info("asQueryable");
        return new AbstractTableQueryable<T>(queryProvider, schema, this, tableName) {
            @Override public Enumerator<T> enumerator() {
                //noinspection unchecked
                return (Enumerator<T>) Linq4j.enumerator(getData());
            }
        };
    }

    private List<Object[]> getData() {
        return table.getData();
    }
}