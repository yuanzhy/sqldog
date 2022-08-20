package com.yuanzhy.sqldog.dialect.hibernate;

import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolver;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/8/20
 */
public class SqldogDialectResolver implements DialectResolver {

    @Override
    public Dialect resolveDialect(DialectResolutionInfo info) {
        if ("sqldog".equalsIgnoreCase(info.getDatabaseName())) {
            return new SqldogDialect();
        }
        return null;
    }
}
