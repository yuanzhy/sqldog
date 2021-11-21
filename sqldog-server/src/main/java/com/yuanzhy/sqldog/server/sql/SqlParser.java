package com.yuanzhy.sqldog.server.sql;

import com.yuanzhy.sqldog.core.util.SqlUtil;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public interface SqlParser {

    SqlCommand parse(String rawSql);

    default String pre(String rawSql) {
        String sql = SqlUtil.stripComments(rawSql, "'\"", "'\"", true, false, true, true);
        sql = SqlUtil.upperCaseIgnoreValue(sql.trim());
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        sql = sql.replaceAll("[\n\r\t]", " ").replaceAll("\\s+", " ").trim();
        return sql;
    }
}
