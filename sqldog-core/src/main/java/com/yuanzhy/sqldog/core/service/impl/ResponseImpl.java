package com.yuanzhy.sqldog.core.service.impl;

import com.yuanzhy.sqldog.core.service.Response;
import com.yuanzhy.sqldog.core.sql.SqlResult;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/18
 */
public class ResponseImpl implements Response {

    private static final long serialVersionUID = 1L;

    private final boolean success;
    private final String message;
    private final SqlResult[] results;

    public ResponseImpl(boolean success) {
        this(success, (String) null);
    }

    public ResponseImpl(boolean success, String message) {
        this(success, message, (SqlResult[]) null);
    }

    public ResponseImpl(boolean success, SqlResult... results) {
        this(success, null, results);
    }

    public ResponseImpl(boolean success, String message, SqlResult... results) {
        this.success = success;
        this.message = message == null ? "" : message;
        this.results = results;
    }

    @Override
    public boolean isSuccess() {
        return success;
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public SqlResult[] getResults() {
        return results;
    }
}
