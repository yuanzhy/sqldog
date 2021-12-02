package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.sql.result.SqlResultBuilder;
import com.yuanzhy.sqldog.server.util.Databases;

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
            checkSchema();
            return builder.labels("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS")
                    .schema(schema.getName())
                    .data(schema.getTableNames().stream().map(t -> {
                        Table table = schema.getTable(t);
                        return new Object[]{dbName, schema.getName(), table.getName(), "table", table.getDescription()}; }).collect(Collectors.toList())
                    )
                    .build();
        } else if ("TABLETYPES".equals(sqlSuffix)) {
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
        } else if ("SEARCH_PATH".equals(sqlSuffix)) {
            checkSchema();
            return builder.schema(schema.getName()).build();
        } else {
            throw new UnsupportedOperationException("not supported: " + sql);
        }
    }
}
