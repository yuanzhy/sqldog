package com.yuanzhy.sqldog.dialect.spring.jdbc;

import org.springframework.data.jdbc.repository.config.DialectResolver;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcOperations;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Optional;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/8/20
 */
public class SqldogDialectProvider implements DialectResolver.JdbcDialectProvider {

    @Override
    public Optional<Dialect> getDialect(JdbcOperations operations) {
        return Optional.ofNullable(operations.execute((ConnectionCallback<Dialect>) SqldogDialectProvider::getDialect));
    }

    private static Dialect getDialect(Connection connection) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        String name = metaData.getDatabaseProductName();
        if ("sqldog".equalsIgnoreCase(name)) {
            return SqldogDialect.INSTANCE;
        }
        return null;
    }
}
