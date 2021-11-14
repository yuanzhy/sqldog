package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.core.SqlCommand;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.core.util.Asserts;
import com.yuanzhy.sqldog.server.memory.ColumnBuilder;
import com.yuanzhy.sqldog.server.util.Databases;
import org.apache.commons.lang3.StringUtils;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public abstract class AbstractSqlCommand implements SqlCommand {

    protected final String sql;

    protected Schema schema;
    protected Table table;

    public AbstractSqlCommand(String sql) {
        this.sql = sql;
    }

    protected void parseSchema(String sqlSuffix) {
        String schemaTable = StringUtils.substringBefore(sqlSuffix, " ");
        if (schemaTable.contains(".")) {
            final String schemaName = StringUtils.substringBefore(schemaTable, ".");
//        final String tableName = StringUtils.substringAfter(schemaTable, ".");
            schema = Databases.getDefault().getSchema(schemaName);
            Asserts.notNull(schema, schemaName + " not exists");
        } else {
            schema = Databases.currSchema();
            Asserts.notNull(schema, "current schema is unset");
        }
    }

    protected String parseTableName(String sqlSuffix) {
        String schemaTable = StringUtils.substringBefore(sqlSuffix, " ");
//        final String schemaName = StringUtils.substringBefore(schemaTable, ".");
        if (schemaTable.contains("(")) {
            schemaTable = StringUtils.substringBefore(schemaTable, "(").trim();
        }
        if (schemaTable.contains(".")) {
            return StringUtils.substringAfter(schemaTable, ".").trim();
        }
        return schemaTable;
    }

    protected void parseSchemaTable(String sqlSuffix) {
        this.parseSchema(sqlSuffix);
        String tableName = parseTableName(sqlSuffix);
        table = schema.getTable(tableName);
        Asserts.notNull(table, tableName + " not exists");
    }

    protected void parsePrecisionAndScale(String rawDataType, ColumnBuilder cb) {
        if (!rawDataType.contains("(") || !rawDataType.contains(")")) {
            throw new IllegalArgumentException("数据长度不能为空");
        }
        String preScale = StringUtils.substringBetween(rawDataType, "(", ")").trim();
        if (preScale.contains(",")) {
            cb.precision(Integer.parseInt(StringUtils.substringBefore(preScale, ",").trim()));
            cb.scale(Integer.parseInt(StringUtils.substringAfter(preScale, ",").trim()));
        } else {
            cb.precision(Integer.parseInt(preScale));
        }
    }

    protected String success() {
        return "(0 rows)";
    }

    protected String success(int rows) {
        return "(" + rows + " rows)";
    }
}
