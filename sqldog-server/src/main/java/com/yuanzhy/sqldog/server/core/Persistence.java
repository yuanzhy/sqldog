package com.yuanzhy.sqldog.server.core;

import com.yuanzhy.sqldog.core.exception.PersistenceException;

import java.util.List;
import java.util.Map;

/**
 * @author yuanzhy
 * @date 2022/3/30
 */
public interface Persistence {

    /**
     *
     * @param relativePath 文件存储标识
     * @return
     * @throws PersistenceException
     */
    Map<String, Object> read(String relativePath) throws PersistenceException;

    /**
     *
     * @param relativePath 文件存储标识
     * @param data 数据
     * @throws PersistenceException
     */
    void write(String relativePath, Map<String, Object> data) throws PersistenceException;

    /**
     *
     * @param relativePath 文件存储标识
     * @throws PersistenceException
     */
    void delete(String relativePath) throws PersistenceException;

    /**
     * 列出子
     * @return 子节点存储标识
     */
    List<String> list(String relativePath) throws PersistenceException;
}
