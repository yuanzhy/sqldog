package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.core.util.StringUtils;
import com.yuanzhy.sqldog.server.core.constant.DataType;
import com.yuanzhy.sqldog.server.sql.result.SqlResultBuilder;
import com.yuanzhy.sqldog.server.storage.builder.ColumnBuilder;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class AlterTableCommand extends AbstractSqlCommand {

    public AlterTableCommand(String sql) {
        super(sql);
    }

    @Override
    public SqlResult execute() {
        // ALTER TABLE schema.table_name ADD column_name datatype;
        // ALTER TABLE schema.table_name DROP COLUMN column_name;
        // ALTER TABLE schema.table_name ALTER COLUMN column_name TYPE datatype;
        // ALTER TABLE schema.table_name ALTER column_name datatype NOT NULL;
        // ALTER TABLE schema.table_name ADD CONSTRAINT MyUniqueConstraint UNIQUE(column1, column2...);
        // ALTER TABLE schema.table_name ADD CONSTRAINT MyPrimaryKey PRIMARY KEY (column1, column2...);
        // ALTER TABLE schema.table_name DROP CONSTRAINT MyUniqueConstraint;
        // ALTER TABLE schema.table_name RENAME TO new_table_name;
        // ALTER TABLE schema.table_name RENAME old_column_name TO new_column_name;
        String tmp = sql.substring("ALTER TABLE ".length());
        super.parseSchemaTable(tmp);
        tmp = StringUtils.substringAfter(tmp, " ");
        if (tmp.startsWith("ADD CONSTRAINT")) {
            throw new UnsupportedOperationException("operation not supported: " + sql);
        } else if (tmp.startsWith("DROP CONSTRAINT")) {
            throw new UnsupportedOperationException("operation not supported: " + sql);
        } else if (tmp.startsWith("ALTER COLUMN")) {
            throw new UnsupportedOperationException("operation not supported: " + sql);
        } else if (tmp.startsWith("ALTER")) {
            throw new UnsupportedOperationException("operation not supported: " + sql);
        } else if (tmp.startsWith("RENAME")) {
            if (tmp.startsWith("RENAME TO")) {
                String newTableName = tmp.substring("RENAME TO ".length()).trim();
                table.rename(newTableName);
            } else {
                String oldColName = StringUtils.substringBetween(tmp, "RENAME ", " TO").trim();
                String newColName = StringUtils.substringAfter(tmp, " TO ").trim();
                if (StringUtils.isAnyEmpty(oldColName, newColName)) {
                    throw new UnsupportedOperationException("operation not supported: " + sql);
                }
                table.renameColumn(oldColName, newColName);
            }
        } else if (tmp.startsWith("DROP COLUMN")) {
            String columnName = tmp.substring("DROP COLUMN ".length());
            table.dropColumn(columnName);
        } else if (tmp.startsWith("ADD")) {
            tmp = tmp.substring("ADD ".length());
            if (tmp.startsWith("COLUMN ")) {
                tmp = tmp.substring("COLUMN ".length()).trim();
            }
            final String columnName = StringUtils.substringBefore(tmp, " ");
            tmp = StringUtils.substringAfter(tmp, " ");
            final String rawDataType = StringUtils.substringBefore(tmp, " ").trim();
            DataType dataType = DataType.of(rawDataType);
            ColumnBuilder cb = new ColumnBuilder().name(columnName).dataType(dataType);
            if (dataType.isHasLength()) {
                super.parsePrecisionAndScale(rawDataType, cb);
            }
            if (tmp.contains(" NOT NULL")) {
                cb.nullable(false);
            }
            if (tmp.contains(" DEFAULT ")) {
                String rawDefault = StringUtils.substringAfter(tmp, " DEFAULT ");
                rawDefault = StringUtils.substringBefore(rawDefault, " ").trim();
                cb.defaultValue(dataType.parseRawValue(rawDefault));
            }
            table.addColumn(cb.build());
        } else {
            throw new UnsupportedOperationException("operation not supported: " + sql);
        }
        return new SqlResultBuilder(StatementType.DDL).schema(currSchema().getName()).table(table.getName()).build();
    }
}
