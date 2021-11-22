package com.yuanzhy.sqldog.server.sql.command;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.sql.result.SqlResultBuilder;
import com.yuanzhy.sqldog.server.util.Calcites;
import com.yuanzhy.sqldog.server.util.Databases;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class SelectCommand extends AbstractSqlCommand {

    public SelectCommand(String sql) {
        super(sql);
    }

    @Override
    public SqlResult execute() {
        try {
            if (schema != null) {
                Databases.currSchema(schema.getName());
            }
            Statement stat = Calcites.getConnection().createStatement();
            ResultSet rs = stat.executeQuery(sql);
            ResultSetMetaData rsmd = rs.getMetaData();
            String[] columnLabels = new String[rsmd.getColumnCount()];
            for (int i = 0; i < rsmd.getColumnCount(); i++) {
                final int index = i + 1;
                //String columnName = rsmd.getColumnName(index);
                String columnLabel = rsmd.getColumnLabel(index); // 别名
                columnLabels[i] = columnLabel;
            }
            // TODO 返回太多卡死内存占用过大问题
            List<Object[]> data = new ArrayList<>();
            while (rs.next()) {
                Object[] values = new Object[columnLabels.length];
                for (int i = 0; i < columnLabels.length; i++) {
                    Object value = rs.getObject(columnLabels[i]);
                    values[i] = value;
                }
                data.add(values);
            }
            return new SqlResultBuilder(StatementType.DQL).rows(data.size()).columns(rsmd).data(data).build();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

//    private String toString(Object value) {
//        if (value instanceof Date) {
//            return DateUtil.formatSqlDate((Date)value);
//        } else if (value instanceof Time) {
//            return DateUtil.formatTime((Time)value);
//        } else if (value instanceof Time) {
//            return DateUtil.formatTime((Time)value);
//        } else if (value instanceof Timestamp) {
//            return DateUtil.formatTimestamp((Timestamp)value);
//        } else if (value instanceof byte[]) {
//            return Base64.getEncoder().encodeToString((byte[])value);
//        } else if (value instanceof Object[]) {
//            return Arrays.toString((Object[])value);
//        } else {
//            return String.valueOf(value);
//        }
//    }
}
