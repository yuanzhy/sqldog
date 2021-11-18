package com.yuanzhy.sqldog.core.rmi;

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
    private final SqlResult result;

    public ResponseImpl(boolean success) {
        this(success, (String) null);
    }

    public ResponseImpl(boolean success, String message) {
        this(success, message, null);
    }

    public ResponseImpl(boolean success, SqlResult result) {
        this(success, null, result);
    }

    public ResponseImpl(boolean success, String message, SqlResult result) {
        this.success = success;
        this.message = message == null ? "" : message;
        this.result = result;
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
    public SqlResult getResult() {
        return result;
    }
}
