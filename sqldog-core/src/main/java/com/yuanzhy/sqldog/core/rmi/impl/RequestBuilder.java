package com.yuanzhy.sqldog.core.rmi.impl;

import com.yuanzhy.sqldog.core.constant.RequestType;
import com.yuanzhy.sqldog.core.rmi.PreparedRequest;
import com.yuanzhy.sqldog.core.rmi.Request;
import com.yuanzhy.sqldog.core.util.Asserts;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/23
 */
public class RequestBuilder {

    private String schema = "PUBLIC";
    private int timeout = 60; // 秒

    private int fetchSize = 0;
    private int offset = 0;
    private RequestType type;
    private String preparedId;
    private String[] sqls;
    private List<Object[]> parameters = new ArrayList<>();

    public RequestBuilder(RequestType type) {
        this.type = type;
    }

    public RequestBuilder schema(String schema) {
        this.schema = schema;
        return this;
    }
    public RequestBuilder timeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    public RequestBuilder fetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }
//    public RequestBuilder type(RequestType type) {
//        this.type = type;
//        return this;
//    }
    public RequestBuilder preparedId(String preparedId) {
        this.preparedId = preparedId;
        return this;
    }

//    public RequestBuilder addSql(String sql) {
//        this.sqls = sqls;
//        return this;
//    }

    public RequestBuilder sqls(String... sqls) {
        this.sqls = sqls;
        return this;
    }

    public RequestBuilder parameter(Object[] parameter) {
        this.parameters.add(parameter);
        return this;
    }

    public RequestBuilder parameters(Object[]... parameters) {
        Collections.addAll(this.parameters, parameters);
        return this;
    }

    public RequestBuilder parameters(List<Object[]> parameters) {
        this.parameters.addAll(parameters);
        return this;
    }

    public RequestBuilder offset(int offset) {
        this.offset = offset;
        return this;
    }

    public Request build() {
        Asserts.notNull(type, "请求类型不能为空");
//        Asserts.notNull(sqls, "sqls不能为空");
        return new RequestImpl(schema, timeout, fetchSize, offset, type, sqls);
    }

    public PreparedRequest buildPrepared() {
        Asserts.notNull(type, "请求类型不能为空");
        Asserts.hasText(preparedId, "preparedId不能为空");
//        Asserts.notNull(sqls, "sqls不能为空");
        PreparedRequestImpl req = new PreparedRequestImpl(schema, timeout, fetchSize, offset, type, preparedId, sqls);
        req.parameters = parameters.toArray(new Object[0][]);
        return req;
    }
}
