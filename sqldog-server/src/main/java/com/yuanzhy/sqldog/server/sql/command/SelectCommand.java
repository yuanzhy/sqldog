package com.yuanzhy.sqldog.server.sql.command;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import com.yuanzhy.sqldog.server.core.util.DateUtil;
import com.yuanzhy.sqldog.server.util.Calcites;
import com.yuanzhy.sqldog.server.util.FormatterUtil;

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
                    if (value instanceof Date) {
                        values[i] = DateUtil.formatSqlDate((Date)value);
                    }
                }
                FormatterUtil.joinByVLine(MAX_LENGTH, columnLabels);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return success();
    }
}
