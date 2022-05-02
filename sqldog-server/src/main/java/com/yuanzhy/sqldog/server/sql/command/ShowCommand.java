package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.constant.TableType;
import com.yuanzhy.sqldog.core.sql.ColumnMetaData;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.core.constant.DataType;
import com.yuanzhy.sqldog.server.sql.result.ColumnMetaDataBuilder;
import com.yuanzhy.sqldog.server.sql.result.SqlResultBuilder;
import com.yuanzhy.sqldog.server.util.Calcites;
import com.yuanzhy.sqldog.server.util.Databases;
import org.apache.calcite.sql.type.SqlTypeName;

import java.sql.Array;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/6
 */
public class ShowCommand extends AbstractSqlCommand {
    public ShowCommand(String sql) {
        super(sql);
    }

    @Override
    public SqlResult execute() {
        String sqlSuffix = sql.substring("SHOW ".length());
        SqlResultBuilder builder = new SqlResultBuilder(StatementType.OTHER);
        String dbName = Databases.getDefault().getName();
        if ("DATABASES".equals(sqlSuffix)) {
            return builder.labels("TABLE_CAT").data(dbName).build();
        } else if ("SCHEMAS".equals(sqlSuffix)) {
            // TODO 支持多库的情况
            return builder
                    .labels("TABLE_CATALOG", "TABLE_SCHEM", "Description")
                    .data(Databases.getDefault().getSchemaNames().stream().map(s -> {
                        Schema schema = Databases.getDefault().getSchema(s);
                        return new Object[]{dbName, schema.getName(), schema.getDescription()}; }).collect(
                            Collectors.toList()))
                    .build();
        } else if ("TABLES".equals(sqlSuffix)) {
//            checkSchema();
            return builder.labels("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS")
                    .schema(currSchema().getName())
                    .data(currSchema().getTableNames().stream().map(t -> {
                        Table table = currSchema().getTable(t);
                        return new Object[]{dbName, currSchema().getName(), table.getName(), TableType.TABLE.getName(), table.getDescription()}; }).collect(Collectors.toList())
                    )
                    .build();
        } else if ("SEARCH_PATH".equals(sqlSuffix)) {
//            checkSchema();
            return builder.schema(currSchema().getName()).build();
        }  else if ("TABLETYPES".equals(sqlSuffix)) {
            /**
             * Retrieves the table types available in this database.  The results
             * are ordered by table type.
             *
             * <P>The table type is:
             *  <OL>
             *  <LI><B>TABLE_TYPE</B> String {@code =>} table type.  Typical types are "TABLE",
             *                  "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY",
             *                  "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
             *  </OL>
             *
             * @return a <code>ResultSet</code> object in which each row has a
             *         single <code>String</code> column that is a table type
             * @exception SQLException if a database access error occurs
             */
            return builder.labels("TABLE_TYPE")
                    .data(new String[]{"TABLE", /*"VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY", "ALIAS", "SYNONYM"*/})
                    .build();
        } else if ("TYPEINFO".equals(sqlSuffix)) {
            ColumnMetaData[] cmdArray = new ColumnMetaData[18];
            cmdArray[0] = this.createColumnMetaData("TYPE_NAME", Types.CHAR, String.class);
            cmdArray[1] = this.createColumnMetaData("DATA_TYPE", Types.INTEGER, Integer.class);
            cmdArray[2] = this.createColumnMetaData("PRECISION", Types.INTEGER, Integer.class);
            cmdArray[3] = this.createColumnMetaData("LITERAL_PREFIX", Types.CHAR, String.class);
            cmdArray[4] = this.createColumnMetaData("LITERAL_SUFFIX", Types.CHAR, String.class);
            cmdArray[5] = this.createColumnMetaData("CREATE_PARAMS", Types.VARCHAR, String.class);
            cmdArray[6] = this.createColumnMetaData("NULLABLE", Types.SMALLINT, Short.class);
            cmdArray[7] = this.createColumnMetaData("CASE_SENSITIVE", Types.BOOLEAN, Boolean.class);
            cmdArray[8] = this.createColumnMetaData("SEARCHABLE", Types.SMALLINT, Short.class);
            cmdArray[9] = this.createColumnMetaData("UNSIGNED_ATTRIBUTE", Types.BOOLEAN, Boolean.class);
            cmdArray[10] = this.createColumnMetaData("FIXED_PREC_SCALE", Types.BOOLEAN, Boolean.class);
            cmdArray[11] = this.createColumnMetaData("AUTO_INCREMENT", Types.BOOLEAN, Boolean.class);
            cmdArray[12] = this.createColumnMetaData("LOCAL_TYPE_NAME", Types.CHAR, String.class);
            cmdArray[13] = this.createColumnMetaData("MINIMUM_SCALE", Types.SMALLINT, Short.class);
            cmdArray[14] = this.createColumnMetaData("MAXIMUM_SCALE", Types.SMALLINT, Short.class);
            cmdArray[15] = this.createColumnMetaData("SQL_DATA_TYPE", Types.INTEGER, Integer.class);
            cmdArray[16] = this.createColumnMetaData("SQL_DATETIME_SUB", Types.INTEGER, Integer.class);
            cmdArray[17] = this.createColumnMetaData("NUM_PREC_RADIX", Types.INTEGER, Integer.class);
            List<Object[]> data = new ArrayList<>();
            for (DataType dt : DataType.values()) {
                String lprefix = null, lsuffix = null;
                if (dt.getClazz() == String.class) {
                    lprefix = "'";
                    lsuffix = "'";
                } else if (dt.getClazz() == Array.class) {
                    lprefix = "[";
                    lsuffix = "]";
                } else if (dt.getClazz() == Object.class) {
                    lprefix = "{";
                    lsuffix = "}";
                }
                boolean unsigned = !(Number.class.isAssignableFrom(dt.getClazz()) && dt != DataType.DATE  && dt != DataType.TIME  && dt != DataType.TIMESTAMP);
                boolean fixedPrecScale = dt == DataType.DECIMAL || dt == DataType.NUMERIC;
                boolean autoIncrement = dt == DataType.SERIAL || dt == DataType.SMALLSERIAL || dt == DataType.BIGSERIAL;
                int maxScale = fixedPrecScale ? 300 : 0;
                Object[] row = new Object[]{dt.name(), dt.getSqlType(), dt.getMaxLength(), lprefix, lsuffix, "", DatabaseMetaData.columnNullable,
                        false, DatabaseMetaData.typeSearchable, unsigned, fixedPrecScale, autoIncrement, "", 0, maxScale, 0, 0, 10 };
                data.add(row);
            }
            return builder.columns(cmdArray).data(data).build();
        } else if ("FUNCTIONS".equals(sqlSuffix)) {
            // *  <LI><B>FUNCTION_CAT</B> String {@code =>} function catalog (may be <code>null</code>)
            //     *  <LI><B>FUNCTION_SCHEM</B> String {@code =>} function schema (may be <code>null</code>)
            //     *  <LI><B>FUNCTION_NAME</B> String {@code =>} function name.  This is the name
            //     * used to invoke the function
            //     *  <LI><B>REMARKS</B> String {@code =>} explanatory comment on the function
            //     * <LI><B>FUNCTION_TYPE</B> short {@code =>} kind of function:
            //     *      <UL>
            //     *      <LI>functionResultUnknown - Cannot determine if a return value
            //     *       or table will be returned
            //     *      <LI> functionNoTable- Does not return a table
            //     *      <LI> functionReturnsTable - Returns a table
            //     *      </UL>
            //     *  <LI><B>SPECIFIC_NAME</B> String  {@code =>} the name which uniquely identifies
            ColumnMetaData[] cmdArray = new ColumnMetaData[6];
            cmdArray[0] = this.createColumnMetaData("FUNCTION_CAT", Types.VARCHAR, String.class);
            cmdArray[1] = this.createColumnMetaData("FUNCTION_SCHEM", Types.VARCHAR, Integer.class);
            cmdArray[2] = this.createColumnMetaData("FUNCTION_NAME", Types.VARCHAR, Integer.class);
            cmdArray[3] = this.createColumnMetaData("REMARKS", Types.VARCHAR, String.class);
            cmdArray[4] = this.createColumnMetaData("FUNCTION_TYPE", Types.SMALLINT, String.class);
            cmdArray[5] = this.createColumnMetaData("SPECIFIC_NAME", Types.VARCHAR, String.class);
            List<Object[]> data = new ArrayList<>();
            for (String functionName : Calcites.getRootSchema().getFunctionNames()) {
                Object[] row = new Object[]{null, null, functionName, null, DatabaseMetaData.functionNoTable, functionName};
                data.add(row);
            }
            return builder.columns(cmdArray).data(data).build();
        } else {
            throw new UnsupportedOperationException("not supported: " + sql);
        }
    }

    private ColumnMetaData createColumnMetaData(String columnName, int columnType, Class<?> clazz) {
        SqlTypeName sqlTypeName = SqlTypeName.getNameForJdbcType(columnType);
        return new ColumnMetaDataBuilder()
                .columnName(columnName)
                .columnType(Types.CHAR)
                .columnTypeName(sqlTypeName == null ? null : sqlTypeName.getName())
                .columnClassName(clazz.getName())
                .searchable(true)
                .nullable(DatabaseMetaData.columnNullable)
                .autoIncrement(columnName.contains("SERIAL"))
                .build();
    }
}
