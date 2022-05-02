package com.yuanzhy.sqldog.core.rmi.impl;

import com.yuanzhy.sqldog.core.constant.RequestType;
import com.yuanzhy.sqldog.core.rmi.PreparedRequest;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/23
 */
public class PreparedRequestImpl extends RequestImpl implements PreparedRequest {

    private final String preparedId;

    Object[][] parameters;

    PreparedRequestImpl(String schema, int timeoutMillis, int fetchSize, RequestType type, String preparedId, String... sql) {
        super(schema, timeoutMillis, fetchSize, type, sql);
        this.preparedId = preparedId;
    }

    @Override
    public String getPrepareId() {
        return preparedId;
    }

    @Override
    public Object[][] getParameters() {
        return parameters;
    }
}
