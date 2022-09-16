package com.yuanzhy.sqldog.server.sql;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/9/16
 */
public interface PreparedSqlParser extends SqlParser {

    @Override
    PreparedSqlCommand parse(String rawSql);
}
