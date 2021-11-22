package com.yuanzhy.sqldog.jdbc;

import com.yuanzhy.sqldog.core.sql.SqlResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author yuanzhy
 * @date 2021-11-16
 */
public interface SqldogConnection extends Connection {

    void checkClosed() throws SQLException;

    /**
     * 执行sql
     * @param sql  sql
     * @return
     * @throws SQLException
     */
    default SqlResult execute(String sql) throws SQLException {
        return execute(sql, 0);
    }

    default SqlResult[] execute(List<String> sqls) throws SQLException {
        return execute(sqls, 0);
    }

    SqlResult[] execute(List<String> sqls, int timeoutSecond) throws SQLException;

    /**
     * 执行sql
     * @param sql  sql
     * @param timeoutSecond 超时时间
     * @return
     * @throws SQLException
     */
    SqlResult execute(String sql, int timeoutSecond) throws SQLException;

    /**
     * prepared
     * @param preparedSql
     * @return
     * @throws SQLException
     */
    SqlResult prepareExecute(String preparedSql) throws SQLException;

    default SqlResult executePrepared(String preparedSql, Object[] parameter) throws SQLException {
        SqlResult[] results = executePrepared(preparedSql, Collections.singletonList(parameter));
        return results == null ? null : results[0];
    }

    SqlResult[] executePrepared(String preparedSql, List<Object[]> parameterList) throws SQLException;
}
