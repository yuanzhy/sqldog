package com.yuanzhy.sqldog.r2dbc;

import com.yuanzhy.sqldog.core.constant.Consts;

import io.r2dbc.spi.ConnectionMetadata;

class SqldogConnectionMetadata implements ConnectionMetadata {

    static final SqldogConnectionMetadata INSTANCE = new SqldogConnectionMetadata();

    @Override
    public String getDatabaseProductName() {
        return Consts.PRODUCT_NAME;
    }

    @Override
    public String getDatabaseVersion() {
        Package pkg = SqldogConnectionMetadata.class.getPackage();
        return (pkg != null ? pkg.getImplementationVersion() : null);
    }
}
