package com.yuanzhy.sqldog.core.service;

import java.io.Serializable;

import com.yuanzhy.sqldog.core.constant.RequestType;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/23
 */
public interface Request extends Serializable {

    String getSchema();

    /**
     * 超时时间，单位 秒
     * @return
     */
    int getTimeout();

    int getFetchSize();

    int getOffset();

    RequestType getType();

    String[] getSql();

    /**
     * 返回的列
     * @return
     */
    String[] getReturnValues();
}
