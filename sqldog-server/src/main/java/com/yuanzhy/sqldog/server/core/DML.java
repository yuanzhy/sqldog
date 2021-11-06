package com.yuanzhy.sqldog.server.core;

import java.util.Map;

/**
 * TODO 暂不支持联表删除和修改
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public interface DML {

    Object insert(Map<String, Object> values);

    int delete(Object id);

    int update(Map<String, Object> updates, Object id);

    int deleteBy(Map<String, Object> wheres);

    int updateBy(Map<String, Object> updates, Map<String, Object> wheres);
}
