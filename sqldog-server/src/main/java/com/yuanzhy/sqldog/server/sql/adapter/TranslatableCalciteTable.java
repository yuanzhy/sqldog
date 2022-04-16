package com.yuanzhy.sqldog.server.sql.adapter;

import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.core.Table;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Map;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/4
 */
public class TranslatableCalciteTable extends AbstractTable implements TranslatableTable {

    private final Table table;

    public TranslatableCalciteTable(Table table) {
        this.table = table;
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return relOptTable.toRel(context);
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
                    builder.add(entry.getKey(), SqlTypeName.TIMESTAMP);
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
}
