package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.sql.result.SqlResultBuilder;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

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
        String[] colArr, valArr;
        if (sqlSuffix.startsWith("(")) {
            // 说明包含 列明
            String[] arr = StringUtils.substringsBetween(sqlSuffix, "(", ")");
            colArr = arr[0].split(",");
            valArr = arr[1].split(",");
        } else {
            colArr = table.getColumns().keySet().toArray(new String[0]);
            valArr = StringUtils.substringBetween(sqlSuffix, "(", ")").split(",");
        }
        Asserts.isTrue(colArr.length == valArr.length, "sql不合法");
        Map<String, Column> columnMap = table.getColumns();
        Map<String, Object> values = new HashMap<>();
        for (int i = 0; i < colArr.length; i++) {
            final String colName = colArr[i].trim();
            final String rawValue = valArr[i].trim();
            final Object value = columnMap.get(colName).getDataType().parseRawValue(rawValue);
            values.put(colName, value);
        }
        Object pk = table.getDML().insert(values);
        return new SqlResultBuilder(StatementType.DML).schema(schema.getName()).table(table.getName()).rows(1)
                .data(pk).build();
    }
}
