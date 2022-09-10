package com.yuanzhy.sqldog.server.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/12/4
 */
public class CommandUtil {

    public static List<Object[]> resolveResultSet(ResultSet rs, ResultSetMetaData rsmd) throws SQLException {
        // TODO 返回太多卡死内存占用过大问题
        List<Object[]> data = new ArrayList<>();
//        Map<String, Boolean> labelIsDate = new HashMap<>();
        while (rs.next()) {
            Object[] values = new Object[rsmd.getColumnCount()];
            for (int i = 0; i < rsmd.getColumnCount(); i++) {
                final int index = i + 1;
                String columnLabel = rsmd.getColumnLabel(index); // 别名
                String className = rsmd.getColumnClassName(index);
                Object value = null;
                if ("java.sql.Timestamp".equals(className)) {
                    value = rs.getTimestamp(columnLabel);
//                    Long ts = rs.getLong(columnLabel);
//                    if (ts != null) {
//                        Boolean isDate = labelIsDate.computeIfAbsent(columnLabel, k -> {
//                            try {
//                                Schema schema = Databases.getDefault().getSchema(rsmd.getSchemaName(index));
//                                Table table = schema.getTable(rsmd.getTableName(index));
//                                Column column = table.getColumn(rsmd.getColumnName(index));
//                                return column.getDataType() == DataType.DATE;
//                            } catch (SQLException e) {
//                                return false;
//                            }
//                        });
//                        value = isDate ? new Date(ts) : new Timestamp(ts);
//                    }
                }
                    else if ("java.sql.Date".equals(className)) {
                        value = rs.getDate(columnLabel);
//                        if (date != null) {
//                            value = new Date(date.longValue());
//                        }
                    }
                else if ("java.sql.Time".equals(className)) {
                    value = rs.getTime(columnLabel);
                } else {
                    value = rs.getObject(columnLabel);
                }
                values[i] = value;
            }
            data.add(values);
        }
        return data;
    }
}
