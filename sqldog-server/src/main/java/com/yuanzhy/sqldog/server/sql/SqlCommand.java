package com.yuanzhy.sqldog.server.sql;

import com.yuanzhy.sqldog.core.sql.SqlResult;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public interface SqlCommand {
    /**
     * 执行sql
     */
    SqlResult execute();

    void defaultSchema(String schema);

    String getSql();
}
