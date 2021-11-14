package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.server.core.constant.DataType;
import com.yuanzhy.sqldog.server.memory.ColumnBuilder;
import org.apache.commons.lang3.StringUtils;

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
    public String execute() {
        // ALTER TABLE schema.table_name ADD column_name datatype;
        // ALTER TABLE schema.table_name DROP COLUMN column_name;
        // ALTER TABLE schema.table_name ALTER COLUMN column_name TYPE datatype;
        // ALTER TABLE schema.table_name ALTER column_name datatype NOT NULL;
        // ALTER TABLE schema.table_name ADD CONSTRAINT MyUniqueConstraint UNIQUE(column1, column2...);
        // ALTER TABLE schema.table_name ADD CONSTRAINT MyPrimaryKey PRIMARY KEY (column1, column2...);
        // ALTER TABLE schema.table_name DROP CONSTRAINT MyUniqueConstraint;
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
        } else if (tmp.startsWith("DROP COLUMN")) {
            String columnName = tmp.substring("drop column ".length());
            table.dropColumn(columnName);
        } else if (tmp.startsWith("ADD")) {
            tmp = tmp.substring("ADD ".length());
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
                cb.defaultValue(dataType.parseValue(rawDefault));
            }
            table.addColumn(cb.build());
        } else {
            throw new UnsupportedOperationException("operation not supported: " + sql);
        }
        return success();
    }
}
