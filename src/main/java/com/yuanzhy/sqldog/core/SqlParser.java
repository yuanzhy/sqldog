package com.yuanzhy.sqldog.core;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public interface SqlParser {

    SqlCommand parse(String sql);
}
