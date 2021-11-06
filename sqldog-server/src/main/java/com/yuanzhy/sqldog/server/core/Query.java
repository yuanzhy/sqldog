package com.yuanzhy.sqldog.server.core;

import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.SqlNode;

/**
 * 目前仅支持单表简单查询
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public interface Query {

    Map<String, Object> select(Object id);

    List<Map<String, Object>> selectBy(SqlNode sqlNode);
}
