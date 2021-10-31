package com.yuanzhy.sqldog.sql.command;

import com.yuanzhy.sqldog.core.Schema;
import com.yuanzhy.sqldog.core.SqlCommand;
import com.yuanzhy.sqldog.core.Table;
import com.yuanzhy.sqldog.core.constant.DataType;
import com.yuanzhy.sqldog.memory.ColumnBuilder;
import com.yuanzhy.sqldog.util.Asserts;
import com.yuanzhy.sqldog.util.Databases;
import com.yuanzhy.sqldog.util.DateUtil;
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
        final String schemaName = StringUtils.substringBefore(schemaTable, ".");
//        final String tableName = StringUtils.substringAfter(schemaTable, ".");
        schema = Databases.getDefault().getSchema(schemaName);
        Asserts.notNull(schema, schemaName + " not exists");
    }

    protected String parseTableName(String sqlSuffix) {
        String schemaTable = StringUtils.substringBefore(sqlSuffix, " ");
//        final String schemaName = StringUtils.substringBefore(schemaTable, ".");
        String tableName = StringUtils.substringAfter(schemaTable, ".");
        if (tableName.contains("(")) {
            tableName = StringUtils.substringBefore(tableName, "(").trim();
        }
        return tableName;
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

    protected Object parseValue(DataType dt, String rawValue) {
        if (rawValue.startsWith("'")) {
            String defaultValue = StringUtils.substringBetween(rawValue, "'");
            if (dt == DataType.DATE) {
                return DateUtil.parseSqlDate(defaultValue);
            } else if (dt == DataType.TIME) {
                return DateUtil.parseSqlTime(defaultValue);
            } else if (dt == DataType.TIMESTAMP) {
                return DateUtil.parseTimestamp(defaultValue);
            } else if (dt == DataType.CHAR || dt == DataType.VARCHAR) {
                return String.valueOf(defaultValue);
            } else {
                throw new UnsupportedOperationException(rawValue + " not supported");
            }
        } else if (rawValue.startsWith("[")) {
            throw new UnsupportedOperationException(rawValue + " not supported");
        } else if (rawValue.startsWith("{")) {
            return rawValue;
        } else {
            return Integer.parseInt(rawValue);
        }
    }
}
