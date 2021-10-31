package com.yuanzhy.sqldog.sql.command;

import com.yuanzhy.sqldog.core.Column;
import com.yuanzhy.sqldog.util.Asserts;
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
    public void execute() {
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
            colArr = table.getColumn().keySet().toArray(new String[0]);
            valArr = StringUtils.substringBetween(sqlSuffix, "(", ")").split(",");
        }
        Asserts.isTrue(colArr.length == valArr.length, "sql不合法");
        Map<String, Column> columnMap = table.getColumn();
        Map<String, Object> values = new HashMap<>();
        for (int i = 0; i < colArr.length; i++) {
            final String colName = colArr[i].trim();
            final String rawValue = valArr[i].trim();
            final Object value = this.parseValue(columnMap.get(colName).getDataType(), rawValue);
            values.put(colName, value);
        }
        table.getDML().insert(values);
    }
}
