package com.yuanzhy.sqldog.server.sql.adapter.sys;

import com.yuanzhy.sqldog.core.constant.RefGeneration;
import com.yuanzhy.sqldog.core.constant.TableType;
import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.sql.adapter.ObjectArrayEnumerable;
import com.yuanzhy.sqldog.server.util.Databases;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * 存储表的 系统表
 * @author yuanzhy
 * @version 1.0
 * @date 2021/12/4
 */
public class TableSysTable extends AbstractTable implements ScannableTable {

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        List<Object[]> data = new ArrayList<>();
        String cat = Databases.getDefault().getName();
        for (String schemaName : Databases.getDefault().getSchemaNames()) {
            Schema schema = Databases.getDefault().getSchema(schemaName);
            for (String tableName : schema.getTableNames()) {
                Table table = schema.getTable(tableName);
                String pk = StringUtils.join(table.getPkColumnName(), ",");
                Object[] row = new Object[]{cat, schema.getName(), tableName, TableType.TABLE.getName(),
                        table.getDescription(), null, null, null, pk, RefGeneration.USER};
                data.add(row);
            }
        }
        return new ObjectArrayEnumerable(root, data);
    }

    /**
     *  <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
     *  <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
     *  <LI><B>TABLE_NAME</B> String {@code =>} table name
     *  <LI><B>TABLE_TYPE</B> String {@code =>} table type.  Typical types are "TABLE",
     *                  "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY",
     *                  "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
     *  <LI><B>REMARKS</B> String {@code =>} explanatory comment on the table
     *  <LI><B>TYPE_CAT</B> String {@code =>} the types catalog (may be <code>null</code>)
     *  <LI><B>TYPE_SCHEM</B> String {@code =>} the types schema (may be <code>null</code>)
     *  <LI><B>TYPE_NAME</B> String {@code =>} type name (may be <code>null</code>)
     *  <LI><B>SELF_REFERENCING_COL_NAME</B> String {@code =>} name of the designated
     *                  "identifier" column of a typed table (may be <code>null</code>)
     *  <LI><B>REF_GENERATION</B> String {@code =>} specifies how values in
     *                  SELF_REFERENCING_COL_NAME are created. Values are
     *                  "SYSTEM", "USER", "DERIVED". (may be <code>null</code>)
     * @param typeFactory
     * @return
     */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        builder.add("TABLE_CAT", SqlTypeName.VARCHAR);
        builder.add("TABLE_SCHEM", SqlTypeName.VARCHAR);
        builder.add("TABLE_NAME", SqlTypeName.VARCHAR);
        builder.add("TABLE_TYPE", SqlTypeName.VARCHAR);
        builder.add("REMARKS", SqlTypeName.VARCHAR);
        builder.add("TYPE_CAT", SqlTypeName.VARCHAR);
        builder.add("TYPE_SCHEM", SqlTypeName.VARCHAR);
        builder.add("TYPE_NAME", SqlTypeName.VARCHAR);
        builder.add("SELF_REFERENCING_COL_NAME", SqlTypeName.VARCHAR);
        builder.add("REF_GENERATION", SqlTypeName.VARCHAR);
        return builder.build();
    }
}
