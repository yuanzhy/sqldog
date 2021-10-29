package com.yuanzhy.sqldog.core;

import java.util.Map;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public interface Table extends Base {

    Map<String, Column> getColumn();

    DML getDML();

    Query getQuery();
}
