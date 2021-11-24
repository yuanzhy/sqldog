package com.yuanzhy.sqldog.server.sql;

import com.yuanzhy.sqldog.core.constant.Consts;
import com.yuanzhy.sqldog.core.util.SqlUtil;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public interface SqlParser {

    SqlCommand parse(String rawSql);

    default String pre(String rawSql) {
        String sql = SqlUtil.stripComments(rawSql.trim(), "'\"", "'\"", true, false, true, true);
        sql = upperCaseIgnoreValue(sql.trim());
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        sql = sql.replaceAll("[\n\r\t]", " ").replaceAll("\\s+", " ").trim();
        sql = replaceSqlKey(sql);
        return sql;
    }

    default String upperCaseIgnoreValue(String str) {
        StringBuilder sb = new StringBuilder();
        boolean valueToken = false;
        boolean escape = false;
        for (char c : str.toCharArray()) {
            // --- 转义处理 ---
            if (c == Consts.SQL_ESCAPE) {
                escape = true;
                sb.append(c);
                continue;
            }
            if (!escape && c == Consts.SQL_QUOTES) {
                valueToken = !valueToken;
                sb.append(c);
            } else {
                sb.append(valueToken ? c : Character.toUpperCase(c));
            }
            escape = false;
        }
        return sb.toString();
    }

    default String replaceSqlKey(String sql) {
        String[] SQL_KEY = {
                "COUNT", "SUM", "AVG"
        };
        if (sql.contains(" AS ")) {
            for (String s : SQL_KEY) {
                sql = sql.replace(" AS ".concat(s), " AS \"" + s + "\"");
            }
        }
        if (sql.contains(" ORDER BY ")) {
            for (String s : SQL_KEY) {
                sql = sql.replace(" ORDER BY ".concat(s), " ORDER BY \"" + s + "\"");
            }
        }
        return sql;
    }
}
