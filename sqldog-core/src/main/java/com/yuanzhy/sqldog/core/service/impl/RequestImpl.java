package com.yuanzhy.sqldog.core.service.impl;

import com.yuanzhy.sqldog.core.constant.RequestType;
import com.yuanzhy.sqldog.core.service.Request;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/23
 */
public class RequestImpl implements Request {

    private final String schema;
    private final int timeout;

    private final int fetchSize;
    private final int offset;
    private final RequestType type;
    private final String[] sql;

    RequestImpl(String schema, int timeout, int fetchSize, int offset, RequestType type, String... sql) {
        this.schema = schema;
        this.timeout = timeout;
        this.fetchSize = fetchSize;
        this.offset = offset;
        this.type = type;
        this.sql = sql == null ? new String[0] : sql;
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public int getTimeout() {
        return timeout;
    }

    @Override
    public int getFetchSize() {
        return fetchSize;
    }

    @Override
    public int getOffset() {
        return offset;
    }

    @Override
    public RequestType getType() {
        return type;
    }

    @Override
    public String[] getSql() {
        return sql;
    }
}
