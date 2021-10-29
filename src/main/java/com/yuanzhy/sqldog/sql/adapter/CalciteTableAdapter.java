package com.yuanzhy.sqldog.sql.adapter;

import java.util.Map;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import com.yuanzhy.sqldog.core.Column;

/**
 *
 * @author yuanzhy
 * @date 2021-10-26
 */
public class CalciteTableAdapter {

    public Table adapt(com.yuanzhy.sqldog.core.Table table) {
        return new CalciteTable(table.getColumn());
    }

    private class CalciteTable extends AbstractTable {
        private Map<String, Column> columnMap;
        CalciteTable(Map<String, Column> columnMap) {
            this.columnMap = columnMap;
        }
        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            RelDataTypeFactory.Builder builder = typeFactory.builder();
            for (Map.Entry<String, Column> entry : this.columnMap.entrySet()) {
                Column column = entry.getValue();
                switch (column.getDataType()) {
                    case INT:
                    case SERIAL:
                        builder.add(entry.getKey(), new BasicSqlType(new RelDataTypeSystemImpl(){}, SqlTypeName.INTEGER));
                        break;
                    case SMALL_INT:
                    case SMALL_SERIAL:
                        builder.add(entry.getKey(), new BasicSqlType(new RelDataTypeSystemImpl(){}, SqlTypeName.SMALLINT));
                        break;
                    case BIG_INT:
                    case BIG_SERIAL:
                        builder.add(entry.getKey(), new BasicSqlType(new RelDataTypeSystemImpl(){}, SqlTypeName.BIGINT));
                        break;
                    case NUMERIC:
                        builder.add(entry.getKey(), new BasicSqlType(new RelDataTypeSystemImpl(){}, SqlTypeName.DECIMAL, column.getPrecision(), column.getScale()));
                        break;
                    case CHAR:
                        builder.add(entry.getKey(), new BasicSqlType(new RelDataTypeSystemImpl(){}, SqlTypeName.CHAR));
                        break;
                    case VARCHAR:
                        builder.add(entry.getKey(), new BasicSqlType(new RelDataTypeSystemImpl(){}, SqlTypeName.VARCHAR));
                        break;
                    case DATE:
                        builder.add(entry.getKey(), new BasicSqlType(new RelDataTypeSystemImpl(){}, SqlTypeName.DATE));
                        break;
                    case TIME:
                        builder.add(entry.getKey(), new BasicSqlType(new RelDataTypeSystemImpl(){}, SqlTypeName.TIME));
                        break;
                    case TIMESTAMP:
                        builder.add(entry.getKey(), new BasicSqlType(new RelDataTypeSystemImpl(){}, SqlTypeName.TIMESTAMP));
                        break;
                    case BOOLEAN:
                        builder.add(entry.getKey(), new BasicSqlType(new RelDataTypeSystemImpl(){}, SqlTypeName.BOOLEAN));
                        break;
                    case ARRAY:
                        builder.add(entry.getKey(), new BasicSqlType(new RelDataTypeSystemImpl(){}, SqlTypeName.ARRAY));
                        break;
                    //case JSON:
                    //    builder.add(entry.getKey(), new BasicSqlType(new RelDataTypeSystemImpl(){}, SqlTypeName.MAP));
                    //    break;
                    case TEXT:
                    case BYTEA:
                        builder.add(entry.getKey(), new BasicSqlType(new RelDataTypeSystemImpl(){}, SqlTypeName.BINARY));
                        break;
                }
            }
            return builder.build();
        }
    }
}
