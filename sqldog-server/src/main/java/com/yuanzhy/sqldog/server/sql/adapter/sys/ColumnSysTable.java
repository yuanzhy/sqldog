package com.yuanzhy.sqldog.server.sql.adapter.sys;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.core.constant.DataType;
import com.yuanzhy.sqldog.server.core.constant.YesNo;
import com.yuanzhy.sqldog.server.sql.adapter.ObjectArrayEnumerable;
import com.yuanzhy.sqldog.server.util.Databases;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/12/4
 */
public class ColumnSysTable extends AbstractTable implements ScannableTable {

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        List<Object[]> data = new ArrayList<>();
        String cat = Databases.getDefault().getName();
        for (String schemaName : Databases.getDefault().getSchemaNames()) {
            Schema schema = Databases.getDefault().getSchema(schemaName);
            for (String tableName : schema.getTableNames()) {
                Table table = schema.getTable(tableName);
                int ordinal = 0;
                for (Column column : table.getColumns().values()) {
                    DataType dt = column.getDataType();
                    SqlTypeName sqlTypeName = SqlTypeName.getNameForJdbcType(dt.getSqlType());
                    String typeName = sqlTypeName == null ? dt.name() : sqlTypeName.getName();
                    Integer scale = (dt == DataType.DECIMAL || dt == DataType.NUMERIC) ? column.getScale() : null;
                    int nullable = column.isNullable() ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls;
                    String autoIncrement = dt.isSerial() ? YesNo.YES.name() : YesNo.NO.name();
                    Object[] row = new Object[]{cat, schema.getName(), tableName, column.getName(), dt.getSqlType(), typeName,
                            column.getPrecision(), null, scale, 10, nullable, column.getDescription(), column.defaultValue(),
                            0, 0, dt.getMaxLength(), ++ordinal, YesNo.YES.name(), null, null, null, null, autoIncrement, YesNo.NO.name()};
                    data.add(row);
                }
            }
        }
        return new ObjectArrayEnumerable(root, data);
    }
    /*
     *  <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
     *  <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
     *  <LI><B>TABLE_NAME</B> String {@code =>} table name
     *  <LI><B>COLUMN_NAME</B> String {@code =>} column name
     *  <LI><B>DATA_TYPE</B> int {@code =>} SQL type from java.sql.Types
     *  <LI><B>TYPE_NAME</B> String {@code =>} Data source dependent type name,
     *  for a UDT the type name is fully qualified
     *  <LI><B>COLUMN_SIZE</B> int {@code =>} column size.
     *  <LI><B>BUFFER_LENGTH</B> is not used.
     *  <LI><B>DECIMAL_DIGITS</B> int {@code =>} the number of fractional digits. Null is returned for data types where
     * DECIMAL_DIGITS is not applicable.
     *  <LI><B>NUM_PREC_RADIX</B> int {@code =>} Radix (typically either 10 or 2)
     *  <LI><B>NULLABLE</B> int {@code =>} is NULL allowed.
     *      <UL>
     *      <LI> columnNoNulls - might not allow <code>NULL</code> values
     *      <LI> columnNullable - definitely allows <code>NULL</code> values
     *      <LI> columnNullableUnknown - nullability unknown
     *      </UL>
     *  <LI><B>REMARKS</B> String {@code =>} comment describing column (may be <code>null</code>)
     *  <LI><B>COLUMN_DEF</B> String {@code =>} default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be <code>null</code>)
     *  <LI><B>SQL_DATA_TYPE</B> int {@code =>} unused
     *  <LI><B>SQL_DATETIME_SUB</B> int {@code =>} unused
     *  <LI><B>CHAR_OCTET_LENGTH</B> int {@code =>} for char types the
     *       maximum number of bytes in the column
     *  <LI><B>ORDINAL_POSITION</B> int {@code =>} index of column in table
     *      (starting at 1)
     *  <LI><B>IS_NULLABLE</B> String  {@code =>} ISO rules are used to determine the nullability for a column.
     *       <UL>
     *       <LI> YES           --- if the column can include NULLs
     *       <LI> NO            --- if the column cannot include NULLs
     *       <LI> empty string  --- if the nullability for the
     * column is unknown
     *       </UL>
     *  <LI><B>SCOPE_CATALOG</B> String {@code =>} catalog of table that is the scope
     *      of a reference attribute (<code>null</code> if DATA_TYPE isn't REF)
     *  <LI><B>SCOPE_SCHEMA</B> String {@code =>} schema of table that is the scope
     *      of a reference attribute (<code>null</code> if the DATA_TYPE isn't REF)
     *  <LI><B>SCOPE_TABLE</B> String {@code =>} table name that this the scope
     *      of a reference attribute (<code>null</code> if the DATA_TYPE isn't REF)
     *  <LI><B>SOURCE_DATA_TYPE</B> short {@code =>} source type of a distinct type or user-generated
     *      Ref type, SQL type from java.sql.Types (<code>null</code> if DATA_TYPE
     *      isn't DISTINCT or user-generated REF)
     *   <LI><B>IS_AUTOINCREMENT</B> String  {@code =>} Indicates whether this column is auto incremented
     *       <UL>
     *       <LI> YES           --- if the column is auto incremented
     *       <LI> NO            --- if the column is not auto incremented
     *       <LI> empty string  --- if it cannot be determined whether the column is auto incremented
     *       </UL>
     *   <LI><B>IS_GENERATEDCOLUMN</B> String  {@code =>} Indicates whether this is a generated column
     *       <UL>
     *       <LI> YES           --- if this a generated column
     *       <LI> NO            --- if this not a generated column
     *       <LI> empty string  --- if it cannot be determined whether this is a generated column
     *       </UL>
     *  </OL>
     */
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        builder.add("TABLE_CAT", SqlTypeName.VARCHAR);
        builder.add("TABLE_SCHEM", SqlTypeName.VARCHAR);
        builder.add("TABLE_NAME", SqlTypeName.VARCHAR);
        builder.add("COLUMN_NAME", SqlTypeName.VARCHAR);
        builder.add("DATA_TYPE", SqlTypeName.INTEGER);
        builder.add("TYPE_NAME", SqlTypeName.VARCHAR);
        builder.add("COLUMN_SIZE", SqlTypeName.INTEGER);
        builder.add("BUFFER_LENGTH", SqlTypeName.INTEGER);
        builder.add("DECIMAL_DIGITS", SqlTypeName.INTEGER);
        builder.add("NUM_PREC_RADIX", SqlTypeName.INTEGER);
        builder.add("NULLABLE", SqlTypeName.INTEGER);
        builder.add("REMARKS", SqlTypeName.VARCHAR);
        builder.add("COLUMN_DEF", SqlTypeName.VARCHAR);
        builder.add("SQL_DATA_TYPE", SqlTypeName.INTEGER);
        builder.add("SQL_DATETIME_SUB", SqlTypeName.INTEGER);
        builder.add("CHAR_OCTET_LENGTH", SqlTypeName.INTEGER);
        builder.add("ORDINAL_POSITION", SqlTypeName.INTEGER);
        builder.add("IS_NULLABLE", SqlTypeName.VARCHAR);
        builder.add("SCOPE_CATALOG", SqlTypeName.VARCHAR);
        builder.add("SCOPE_SCHEMA", SqlTypeName.VARCHAR);
        builder.add("SCOPE_TABLE", SqlTypeName.VARCHAR);
        builder.add("SOURCE_DATA_TYPE", SqlTypeName.SMALLINT);
        builder.add("IS_AUTOINCREMENT", SqlTypeName.VARCHAR);
        builder.add("IS_GENERATEDCOLUMN", SqlTypeName.VARCHAR);
        return builder.build();
    }
}
