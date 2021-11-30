package com.yuanzhy.sqldog.server.sql.command;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.core.util.SqlUtil;
import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.sql.result.SqlResultBuilder;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class InsertCommand extends AbstractSqlCommand {
    public InsertCommand(String sql) {
        super(sql);
    }

    @Override
    public SqlResult execute() {
        // insert into scheme.table_name (id, name) values(1, 'zs')
        // insert into scheme.table_name values(1, 'zs')
        String sqlSuffix = sql.substring("insert into ".length());
        super.parseSchemaTable(sqlSuffix);
        sqlSuffix = StringUtils.substringAfter(sqlSuffix, table.getName()).trim();
        if (!sqlSuffix.startsWith("VALUES") && !sqlSuffix.startsWith("(")) {
            throw new IllegalArgumentException("Illegal sql: " + sql);
        }
        String[] colArr, valArr;
        if (sqlSuffix.startsWith("(")) {
            // 说明包含 列明
            String colStr = StringUtils.substringBetween(sqlSuffix, "(", ")");
            colArr = colStr.trim().split(",");
            sqlSuffix = StringUtils.substringAfter(sqlSuffix, ")").trim();
            Asserts.isTrue(sqlSuffix.startsWith("VALUES"), "Illegal sql: " + sql);
            sqlSuffix = StringUtils.substringAfter(sqlSuffix, "(");
            sqlSuffix = StringUtils.substringBeforeLast(sqlSuffix, ")");
            valArr = SqlUtil.parseLine(sqlSuffix.trim());
        } else {
            colArr = table.getColumns().keySet().toArray(new String[0]);
            sqlSuffix = StringUtils.substringAfter(sqlSuffix, "(");
            sqlSuffix = StringUtils.substringBeforeLast(sqlSuffix, ")");
            valArr = SqlUtil.parseLine(sqlSuffix.trim());
        }
        Asserts.isTrue(colArr.length == valArr.length, "illegal sql: " + sql);
        Map<String, Column> columnMap = table.getColumns();
        Map<String, Object> values = new HashMap<>();
        for (int i = 0; i < colArr.length; i++) {
            final String colName = colArr[i].trim();
            final String rawValue = valArr[i].trim();
            final Object value = columnMap.get(colName).getDataType().parseValue(rawValue);
            values.put(colName, value);
        }
        Object[] pkValues = table.getDML().insert(values);
        return new SqlResultBuilder(StatementType.DML).schema(schema.getName()).table(table.getName()).rows(1)
                .labels(table.getPkNames())
                .data(pkValues).build();
    }
}
