package com.yuanzhy.sqldog.server.sql.adapter;

import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.core.Table;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/4
 */
public class FilterableCalciteTable extends AbstractQueryableTable implements FilterableTable, ModifiableTable {
    private static final Logger LOG = LoggerFactory.getLogger(FilterableCalciteTable.class);
    private static final Type TYPE = Object[].class;

    private final Table table;

    public FilterableCalciteTable(Table table) {
        super(TYPE);
        Asserts.hasEle(table.getColumns(), "column is empty");
        this.table = table;
    }

    @Override
    public Statistic getStatistic() {
        return Statistics.of(table.getTableData().getCount(), null);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        Map<String, Column> columnMap = table.getColumns();
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
                case TINYINT:
                    builder.add(entry.getKey(), SqlTypeName.TINYINT);
                    break;
                case FLOAT:
                    builder.add(entry.getKey(), SqlTypeName.FLOAT);
                    break;
                case DOUBLE:
                    builder.add(entry.getKey(), SqlTypeName.DOUBLE);
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
                case TEXT: // TODO text
                    builder.add(entry.getKey(), SqlTypeName.VARCHAR, Integer.MAX_VALUE);
                    break;
                case DATE:
//                    builder.add(entry.getKey(), SqlTypeName.DATE);
                    builder.add(entry.getKey(), typeFactory.createJavaType(Date.class));
                    break;
                case TIME:
//                    builder.add(entry.getKey(), SqlTypeName.TIME);
                    builder.add(entry.getKey(), typeFactory.createJavaType(Time.class));
                    break;
                case TIMESTAMP:
//                    builder.add(entry.getKey(), SqlTypeName.TIMESTAMP);
                    builder.add(entry.getKey(), typeFactory.createJavaType(Timestamp.class));
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
                case BYTEA:
                    builder.add(entry.getKey(), SqlTypeName.BINARY);
                    break;
                default:
                    //builder.add(entry.getKey(), SqlTypeName.ANY);
                    throw new IllegalArgumentException("unknown data type");
            }
            builder.nullable(column.isNullable());
        }
        return builder.build();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new FilterableEnumerator(root, table.getTableData(), filters);
            }
        };
//        for (RexNode filter : filters) {
//            if (filter instanceof RexCall) {
//                RexCall call = (RexCall) filter;
////                call.getOperands()
//            }
//        }
//        JavaTypeFactory typeFactory = root.getTypeFactory();
//        final List<RelDataType> fieldTypes = getFieldTypes(typeFactory);
//        final String[] filterValues = new String[fieldTypes.size()];
//        filters.removeIf(filter -> addFilter(filter, filterValues));
//        final List<Integer> fields = ImmutableIntList.identity(fieldTypes.size());
//        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
////        return new AbstractEnumerable<@Nullable Object[]>() {
////            @Override public Enumerator<@Nullable Object[]> enumerator() {
////                return new CsvEnumerator<>(source, cancelFlag, false, filterValues,
////                        CsvEnumerator.arrayConverter(fieldTypes, fields, false));
////            }
////        };
//
//        return new ObjectArrayEnumerable(root, table.getTableData().getData());
    }

//    private static boolean addFilter(RexNode filter, Object[] filterValues) {
//        if (filter.isA(SqlKind.AND)) {
//            // We cannot refine(remove) the operands of AND,
//            // it will cause o.a.c.i.TableScanNode.createFilterable filters check failed.
//            ((RexCall) filter).getOperands().forEach(subFilter -> addFilter(subFilter, filterValues));
//        } else if (filter.isA(SqlKind.EQUALS)) {
//            final RexCall call = (RexCall) filter;
//            RexNode left = call.getOperands().get(0);
//            if (left.isA(SqlKind.CAST)) {
//                left = ((RexCall) left).operands.get(0);
//            }
//            final RexNode right = call.getOperands().get(1);
//            if (left instanceof RexInputRef
//                    && right instanceof RexLiteral) {
//                final int index = ((RexInputRef) left).getIndex();
//                if (filterValues[index] == null) {
//                    filterValues[index] = ((RexLiteral) right).getValue2().toString();
//                    return true;
//                }
//            }
//        }
//        return false;
//    }
    @Override
    public Collection getModifiableCollection() {
    return null;
}

    @Override
    public TableModify toModificationRel(RelOptCluster cluster, RelOptTable table, Prepare.CatalogReader catalogReader, RelNode child, TableModify.Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened) {
        return LogicalTableModify.create(table, catalogReader, child, operation,
                updateColumnList, sourceExpressionList, flattened);
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schemaPlus, String tableName) {
        return new AbstractTableQueryable<T>(queryProvider, schemaPlus, this, tableName) {
            @Override public Enumerator<T> enumerator() {
                //noinspection unchecked
                return (Enumerator<T>) Linq4j.iterableEnumerator(FilterableCalciteTable.this.table.getTableData());
            }
        };
    }
}
