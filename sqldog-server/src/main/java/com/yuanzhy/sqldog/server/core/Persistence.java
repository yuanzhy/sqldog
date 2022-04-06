package com.yuanzhy.sqldog.server.core;

import com.yuanzhy.sqldog.core.exception.PersistenceException;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author yuanzhy
 * @date 2022/3/30
 */
public interface Persistence {

    /**
     *
     * @param storagePath 文件存储标识
     * @return
     * @throws PersistenceException
     */
    Map<String, Object> readMeta(String storagePath) throws PersistenceException;

    /**
     *
     * @param storagePath 文件存储标识
     * @param data 数据
     * @throws PersistenceException
     */
    void writeMeta(String storagePath, Map<String, Object> data) throws PersistenceException;

    /**
     *
     * @param storagePath 文件存储标识
     * @throws PersistenceException
     */
    void delete(String storagePath) throws PersistenceException;

    /**
     * 列出子
     * @return 子节点存储标识
     */
    List<String> list(String storagePath) throws PersistenceException;

    /**
     *
     * @param paths 各个域的名称
     * @return storagePath
     */
    String resolvePath(String... paths);

    default String resolvePath(Base base, String... extraPaths) {
        LinkedList<String> list = new LinkedList<>();
        list.add(base.getName());
        Base parent = base;
        while ((parent = parent.getParent()) != null) {
            list.addFirst(parent.getName());
        }
        if (ArrayUtils.isNotEmpty(extraPaths)) {
            Collections.addAll(list, extraPaths);
        }
        return resolvePath(list.toArray(ArrayUtils.EMPTY_STRING_ARRAY));
    }
}
