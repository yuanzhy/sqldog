package com.yuanzhy.sqldog.core.service.impl;

import com.yuanzhy.sqldog.core.constant.RequestType;
import com.yuanzhy.sqldog.core.service.PreparedRequest;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/23
 */
public class PreparedRequestImpl extends RequestImpl implements PreparedRequest {

    private static final long serialVersionUID = 1L;

    private final String preparedId;

    Object[][] parameters;

    PreparedRequestImpl(String schema, int timeoutMillis, int fetchSize, int offset, RequestType type, String preparedId, String[] sql, String[] returnValues) {
        super(schema, timeoutMillis, fetchSize, offset, type, sql, returnValues);
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
