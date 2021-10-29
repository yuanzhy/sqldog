package com.yuanzhy.sqldog.sql.adapter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.yuanzhy.sqldog.core.Column;
import com.yuanzhy.sqldog.core.Table;

/**
 *
 * @author yuanzhy
 * @date 2021-10-26
 */
public class CalciteTableAdapter {

    public org.apache.calcite.schema.Table adapt(Table table) {
        return new CalciteTable(table);
    }
    // 文件表需要实现 org.apache.calcite.schema.FilterableTable
    private class CalciteTable extends AbstractTable implements ScannableTable {
        private Table table;
        private CalciteTable(Table table) {
            this.table = table;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            Map<String, Column> columnMap = table.getColumn();
            if (columnMap.isEmpty()) {
                throw new IllegalArgumentException("column is empty");
            }
            RelDataTypeFactory.Builder builder = typeFactory.builder();
            for (Map.Entry<String, Column> entry : columnMap.entrySet()) {
                Column column = entry.getValue();
                switch (column.getDataType()) {
                    case INT:
                    case SERIAL:
                        builder.add(entry.getKey(), SqlTypeName.INTEGER);
                        break;
                    case SMALL_INT:
                    case SMALL_SERIAL:
                        builder.add(entry.getKey(), SqlTypeName.SMALLINT);
                        break;
                    case BIG_INT:
                    case BIG_SERIAL:
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
            final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
            return new AbstractEnumerable<Object[]>() {
                @Override
                public Enumerator<Object[]> enumerator() {
                    return new Enumerator<Object[]>() {
                        private int index = -1;
                        private List<Object[]> data = table.getData();
                        @Override
                        public Object[] current() {
                            Object[] current = this.data.get(this.index);
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
    }

}
