package com.yuanzhy.sqldog.server.sql.command;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.core.util.ArrayUtils;
import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.core.util.SqlUtil;
import com.yuanzhy.sqldog.core.util.StringUtils;
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
        sqlSuffix = stripQuotes(sqlSuffix);
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
        Asserts.isTrue(valArr.length % colArr.length == 0, "illegal sql: " + sql);
        // todo 此处先简单支持多值， 后续insert全部改为状态机解析
        List<String[]> rows;
        if (colArr.length < valArr.length) {
            int start = 0;
            rows = new ArrayList<>();
            int c = valArr.length / colArr.length;
            for (int i = 0; i < c; i++) {
                int end = start + colArr.length - 1;
                if (valArr[start].startsWith("(")) {
                    valArr[start] = valArr[start].substring(1);
                }
                if (valArr[end].endsWith(")")) {
                    valArr[end] = valArr[end].substring(0, valArr[end].length() - 1);
                }
                rows.add(ArrayUtils.subarray(valArr, start, end + 1));
                start += colArr.length;
            }
        } else {
            rows = Collections.singletonList(valArr);
        }
        Map<String, Column> columnMap = table.getColumns();
        List<Object[]> pkValuesList = new ArrayList<>(rows.size());
        for (String[] row : rows) {
            Map<String, Object> values = new HashMap<>();
            for (int i = 0; i < colArr.length; i++) {
                final String colName = stripQuotes(colArr[i]);
                final String rawValue = row[i].trim();
                final Object value = columnMap.get(colName).getDataType().parseValue(rawValue);
                values.put(colName, value);
            }
            Object[] pkValues = table.getTableData().insert(values);
            pkValuesList.add(pkValues);
        }
        return new SqlResultBuilder(StatementType.DML).schema(currSchema().getName()).table(table.getName()).rows(rows.size())
                .labels(table.getPkColumnName())
                .data(pkValuesList).build();
    }
}
