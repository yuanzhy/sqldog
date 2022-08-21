package com.yuanzhy.sqldog.server.sql;

import java.io.Closeable;

import com.yuanzhy.sqldog.core.sql.SqlResult;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/21
 */
public interface PreparedSqlCommand extends SqlCommand, Closeable {

    SqlResult execute(Object[] parameter);
}
