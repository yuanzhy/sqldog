package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.core.util.DateUtil;
import com.yuanzhy.sqldog.server.util.Calcites;
import com.yuanzhy.sqldog.server.util.FormatterUtil;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Base64;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class SelectCommand extends AbstractSqlCommand {

    private static final int MAX_LENGTH = 20;

    public SelectCommand(String sql) {
        super(sql);
    }

    @Override
    public String execute() {
        try {
            Statement stat = Calcites.getConnection().createStatement();
            ResultSet rs = stat.executeQuery(sql);
            ResultSetMetaData rsmd = rs.getMetaData();
            String[] columnLabels = new String[rsmd.getColumnCount()];
            for (int i = 0; i < rsmd.getColumnCount(); i++) {
                //String columnName = rsmd.getColumnName(i+1);
                String columnLabel = rsmd.getColumnLabel(i+1); // 别名
                columnLabels[i] = columnLabel;
            }
            // TODO 返回太多卡死内存占用过大问题
            StringBuilder sb = new StringBuilder();
            sb.append(FormatterUtil.joinByVLine(MAX_LENGTH, columnLabels)).append("\n");
            sb.append(FormatterUtil.genHLine(MAX_LENGTH, rsmd.getColumnCount())).append("\n");

                    //columnMap.values().stream().map(Column::toPrettyString).collect(Collectors.joining("\n")) + "\n" +
                    //"Constraint:\n" +
                    //"    " + primaryKey.toPrettyString() + "\n" +
                    //constraint.stream().map(Constraint::toPrettyString).map(s -> "    " + s).collect(Collectors.joining("\n"))
                    //;
            int count = 0;
            while (rs.next()) {
                count++;
                String[] values = new String[columnLabels.length];
                for (int i = 0; i < columnLabels.length; i++) {
                    Object value = rs.getObject(columnLabels[i]);
                    values[i] = this.toString(value);
                }
                sb.append(FormatterUtil.joinByVLine(MAX_LENGTH, values)).append("\n");
            }
            sb.append("\n");
            sb.append(success(count));
            return sb.toString();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String toString(Object value) {
        if (value instanceof Date) {
            return DateUtil.formatSqlDate((Date)value);
        } else if (value instanceof Time) {
            return DateUtil.formatTime((Time)value);
        } else if (value instanceof Time) {
            return DateUtil.formatTime((Time)value);
        } else if (value instanceof Timestamp) {
            return DateUtil.formatTimestamp((Timestamp)value);
        } else if (value instanceof byte[]) {
            return Base64.getEncoder().encodeToString((byte[])value);
        } else if (value instanceof Object[]) {
            return Arrays.toString((Object[])value);
        } else {
            return String.valueOf(value);
        }
    }
}
