package com.yuanzhy.sqldog.server.sql.command;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.sql.result.SqlResultBuilder;
import com.yuanzhy.sqldog.server.util.Calcites;
import com.yuanzhy.sqldog.server.util.CommandUtil;

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
            // 优化，查询总数的情况下直接给出结果
            if ((sql.startsWith("SELECT COUNT(*) FROM ") || sql.startsWith("SELECT COUNT (*) FROM ")) && !sql.contains("WHERE") && !sql.contains("GROUP BY")) {
                super.parseSchemaTable(sql.substring("SELECT COUNT(*) FROM ".length()).trim());
                return new SqlResultBuilder(StatementType.DQL).rows(1).labels("COUNT(*)").data(table.getTableData().getCount()).build();
            }
            Statement stat = Calcites.getConnection().createStatement();
            ResultSet rs = stat.executeQuery(sql);
            ResultSetMetaData rsmd = rs.getMetaData();
            List<Object[]> data = CommandUtil.resolveResultSet(rs, rsmd);
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
