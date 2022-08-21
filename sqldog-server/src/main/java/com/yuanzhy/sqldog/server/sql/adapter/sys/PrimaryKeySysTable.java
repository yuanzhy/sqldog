package com.yuanzhy.sqldog.server.sql.adapter.sys;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.yuanzhy.sqldog.server.core.Constraint;
import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.sql.adapter.ObjectArrayEnumerable;
import com.yuanzhy.sqldog.server.util.Databases;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/12/4
 */
public class PrimaryKeySysTable extends AbstractTable implements ScannableTable {
    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        List<Object[]> data = new ArrayList<>();
        String cat = Databases.getDefault().getName();
        for (String schemaName : Databases.getDefault().getSchemaNames()) {
            Schema schema = Databases.getDefault().getSchema(schemaName);
            for (String tableName : schema.getTableNames()) {
                Table table = schema.getTable(tableName);
                Constraint primaryKey = table.getPrimaryKey();
                if (primaryKey == null) {
                    continue;
                }
                String[] colName = primaryKey.getColumnNames();
                for (int i = 0; i < colName.length; i++) {
                    Object[] row = new Object[]{cat, schema.getName(), tableName, colName[i], i+1, primaryKey.getName()};
                    data.add(row);
                }
            }
        }
        return new ObjectArrayEnumerable(root, data);
    }

    /**
     *  <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
     *  <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
     *  <LI><B>TABLE_NAME</B> String {@code =>} table name
     *  <LI><B>COLUMN_NAME</B> String {@code =>} column name
     *  <LI><B>KEY_SEQ</B> short {@code =>} sequence number within primary key( a value
     *  of 1 represents the first column of the primary key, a value of 2 would
     *  represent the second column within the primary key).
     *  <LI><B>PK_NAME</B> String {@code =>} primary key name (may be <code>null</code>)
     */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        builder.add("TABLE_CAT", SqlTypeName.VARCHAR);
        builder.add("TABLE_SCHEM", SqlTypeName.VARCHAR);
        builder.add("TABLE_NAME", SqlTypeName.VARCHAR);
        builder.add("COLUMN_NAME", SqlTypeName.VARCHAR);
        builder.add("KEY_SEQ", SqlTypeName.SMALLINT);
        builder.add("PK_NAME", SqlTypeName.VARCHAR);
        return builder.build();
    }
}
