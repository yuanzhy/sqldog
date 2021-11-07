package com.yuanzhy.sqldog.server.core;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public interface Base {

    String getName();

    String getDescription();

    void setDescription(String description);

    String toPrettyString();

    /**
     * 删除后的清理操作
     */
    void drop();
}
