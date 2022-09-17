package com.yuanzhy.sqldog.r2dbc;

import java.rmi.RemoteException;

import io.r2dbc.spi.R2dbcException;

class SQLError {

    public static R2dbcException wrapEx(Throwable e) {
        if (e instanceof R2dbcException) {
            return (R2dbcException) e;
        }
        if (e instanceof RuntimeException || e instanceof RemoteException) {
            e = e.getCause();
        }
        if (e.getCause() == null) {
            return new R2dbcConnectionException(e.getMessage(), e);
        }
        return new R2dbcConnectionException(e.getCause().getMessage(), e.getCause());
    }
}
