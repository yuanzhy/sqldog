package com.yuanzhy.sqldog.core.constant;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/6
 */
public enum Auth {

    SUCCESS("Authentication Success"),
    ILLEGAL("Illegal auth params"),
    FAILURE("Authentication failure");

    private String msg;

    Auth(String msg) {
        this.msg = msg;
    }

    public String value() {
        return msg;
    }
}
