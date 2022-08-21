package com.yuanzhy.sqldog.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;

import com.yuanzhy.sqldog.core.sql.SqlResult;

/**
 *
 * @author yuanzhy
 * @date 2021-11-16
 */
public interface SqldogConnection extends Connection {

    void checkClosed() throws SQLException;

    void close(Statement statement);

    SqlResult[] execute(Statement statement, int offset, String... sqls) throws SQLException;

    /**
     * 执行sql
     * @param statement statement
     * @param offset  offset, use to fetchSize
     * @param sql  sql
     * @return
     * @throws SQLException
     */
    SqlResult execute(Statement statement, int offset, String sql) throws SQLException;

    default SqlResult execute(Statement statement, String sql) throws SQLException {
        return execute(statement, 0, sql);
    }
    /**
     * prepared
     * @param preparedSql
     * @return
     * @throws SQLException
     */
    SqlResult prepareExecute(PreparedStatement statement, String preparedId, String preparedSql) throws SQLException;

    default SqlResult executePrepared(PreparedStatement statement, int offset, String preparedId, String preparedSql, Object[] parameter) throws SQLException {
        SqlResult[] results = executePrepared(statement, offset, preparedId, preparedSql, Collections.singletonList(parameter));
        return results == null ? null : results[0];
    }

    SqlResult[] executePrepared(PreparedStatement statement, int offset, String preparedId, String preparedSql, List<Object[]> parameterList) throws SQLException;
}
