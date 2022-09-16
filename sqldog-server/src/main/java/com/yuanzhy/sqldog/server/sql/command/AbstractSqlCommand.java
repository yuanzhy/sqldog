package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.core.util.StringUtils;
import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.sql.SqlCommand;
import com.yuanzhy.sqldog.server.storage.builder.ColumnBuilder;
import com.yuanzhy.sqldog.server.util.Databases;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public abstract class AbstractSqlCommand implements SqlCommand {

    protected final String sql;

    protected Schema defaultSchema;
    protected Schema sqlSchema;
    protected Table table;

    public AbstractSqlCommand(String sql) {
        this.sql = sql;
    }

    @Override
    public void defaultSchema(String schema) {
        if (StringUtils.isEmpty(schema)) {
            schema = StorageConst.DEF_SCHEMA_NAME;
        }
        this.defaultSchema = Databases.getDefault().getSchema(schema);
        Asserts.notNull(this.defaultSchema, "current schema is unset");
    }

    @Override
    public String getSql() {
        return sql;
    }

    /**
     * 获取当前语句的schema, 如果sql中没指定则获取default
     */
    protected Schema currSchema() {
        return sqlSchema == null ? defaultSchema : sqlSchema;
    }

    protected void parseSchema(String sqlSuffix) {
        String schemaTable = StringUtils.substringBefore(sqlSuffix, " ");
        if (schemaTable.contains(".")) {
            String schemaName = StringUtils.substringBefore(schemaTable, ".");
            schemaName = stripQuotes(schemaName);
//        final String tableName = StringUtils.substringAfter(schemaTable, ".");
            sqlSchema = Databases.getDefault().getSchema(schemaName);
            Asserts.notNull(sqlSchema, schemaName + " not exists");
        }
    }

    protected String stripQuotes(String sqlIdentify) {
        return StringUtils.strip(sqlIdentify, "\"` ");
    }

    protected String parseTableName(String sqlSuffix) {
        String schemaTable = StringUtils.substringBefore(sqlSuffix, " ");
//        final String schemaName = StringUtils.substringBefore(schemaTable, ".");
        if (schemaTable.contains("(")) {
            schemaTable = StringUtils.substringBefore(schemaTable, "(").trim();
        }
        if (schemaTable.contains(".")) {
            schemaTable = StringUtils.substringAfter(schemaTable, ".").trim();
        }
        return stripQuotes(schemaTable);
    }

    protected void parseSchemaTable(String sqlSuffix) {
        this.parseSchema(sqlSuffix);
        String tableName = parseTableName(sqlSuffix);
        table = currSchema().getTable(tableName);
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
}
