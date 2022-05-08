package com.yuanzhy.sqldog.core.rmi;

import com.yuanzhy.sqldog.core.constant.RequestType;

import java.io.Serializable;

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
}
