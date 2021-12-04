package com.yuanzhy.sqldog.server.sql.adapter.sys;

import com.yuanzhy.sqldog.server.sql.adapter.ObjectArrayEnumerable;
import com.yuanzhy.sqldog.server.util.Databases;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/12/4
 */
public class SchemaSysTable extends AbstractTable implements ScannableTable {
    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        List<Object[]> data = new ArrayList<>();
        String cat = Databases.getDefault().getName();
        for (String schemaName : Databases.getDefault().getSchemaNames()) {
            Object[] row = new Object[]{cat, schemaName};
            data.add(row);
        }
        return new ObjectArrayEnumerable(root, data);
    }

    /*
     * <P>The schema columns are:
     *  <OL>
     *  <LI><B>TABLE_SCHEM</B> String {@code =>} schema name
     *  <LI><B>TABLE_CATALOG</B> String {@code =>} catalog name (may be <code>null</code>)
     *  </OL>
     */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        builder.add("TABLE_CATALOG", SqlTypeName.VARCHAR);
        builder.add("TABLE_SCHEM", SqlTypeName.VARCHAR);
        return builder.build();
    }
}
