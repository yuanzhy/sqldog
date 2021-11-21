package com.yuanzhy.sqldog.jdbc.impl;

import java.sql.SQLException;
import java.sql.Wrapper;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/20
 */
abstract class AbstractWrapper implements Wrapper {

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("does not implement '" + iface + "'");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
