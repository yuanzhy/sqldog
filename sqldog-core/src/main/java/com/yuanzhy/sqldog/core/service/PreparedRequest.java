package com.yuanzhy.sqldog.core.service;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/23
 */
public interface PreparedRequest extends Request {

    String getPrepareId();

    Object[][] getParameters();
}
