package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.sql.result.SqlResultBuilder;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author yuanzhy
 * @date 2021-11-02
 */
public class CommentCommand extends AbstractSqlCommand {

    public CommentCommand(String sql) {
        super(sql);
    }

    @Override
    public SqlResult execute() {
        // comment on table schema.table is '一个神奇的表';
        // comment on column schema.table.column is '一个神奇的列';
        String type = sql.substring("COMMENT ON ".length());
        if (type.startsWith("TABLE")) {
            String sqlSuffix = type.substring("TABLE ".length());
            super.parseSchemaTable(sqlSuffix);
            table.setDescription(StringUtils.substringBetween(sqlSuffix, "'"));
        } else if (type.startsWith("COLUMN")) {
            String sqlSuffix = type.substring("COLUMN ".length());
            String schemaTableColumn = StringUtils.substringBefore(sqlSuffix, " IS");
            String schemaTable = StringUtils.substringBeforeLast(schemaTableColumn, ".").trim();
            super.parseSchemaTable(schemaTable);
            String colName = StringUtils.substringAfterLast(schemaTableColumn, ".").trim();
            table.getColumn(colName).setDescription(StringUtils.substringBetween(sqlSuffix, "'"));
        } else {
            throw new UnsupportedOperationException("not supported: " + sql);
        }
        return new SqlResultBuilder(StatementType.DDL).schema(schema.getName()).table(table.getName()).build();
    }
}