package com.yuanzhy.sqldog.server.sql;

import com.yuanzhy.sqldog.core.sql.SqlResult;

import java.io.Closeable;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/21
 */
public interface PreparedSqlCommand extends SqlCommand, Closeable {

    SqlResult execute(Object[] parameters);
}
