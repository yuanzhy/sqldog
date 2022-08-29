package com.yuanzhy.sqldog.server.core;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.yuanzhy.sqldog.core.exception.PersistenceException;
import com.yuanzhy.sqldog.core.util.ArrayUtils;
import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.common.model.DataPage;
import com.yuanzhy.sqldog.server.common.model.IndexPage;
import com.yuanzhy.sqldog.server.common.model.LeafIndexPage;

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
     * 读取一个数据页
     * @param tablePath table路径
     * @param fileId    文件标识
     * @param offset    偏移量
     * @return DataPage: nullable
     */
    DataPage readPage(String tablePath, short fileId, int offset) throws PersistenceException;

    /**
     * 获取可插入的数据页
     * @param tablePath table路径
     * @return DataPage: not null
     */
    DataPage getInsertablePage(String tablePath) throws PersistenceException;

    /**
     * 写入数据页
     * @param tablePath table路径
     * @param dataPage 数据页
     * @return 写入后的数据页
     */
    DataPage writePage(String tablePath, DataPage dataPage) throws PersistenceException;

    /**
     * 读取大字段
     * @param tablePath table路径
     * @param extraId 代表该数据存储位置的标识
     * @return 数据
     */
    byte[] readExtraData(String tablePath, byte[] extraId) throws PersistenceException;
    /**
     * 写入大字段
     * @param tablePath table路径
     * @param bytes     数据
     * @return 代表该数据存储位置的标识
     * @throws PersistenceException
     */
    byte[] writeExtraData(String tablePath, byte[] bytes) throws PersistenceException;

    /**
     * 对应位置写入索引页，相当于覆盖一个页
     * @throws PersistenceException
     */
    void writeIndex(String tablePath, String colName, IndexPage indexPage) throws PersistenceException;

    IndexPage newIndex(String tablePath, String colName, int level) throws PersistenceException;

    IndexPage getInsertableIndex(String tablePath, String colName, int level) throws PersistenceException;

    default LeafIndexPage getInsertableLeafIndex(String tablePath, String colName) throws PersistenceException {
        return (LeafIndexPage) getInsertableIndex(tablePath, colName, 0);
    }
    /**
     * 读取索引页
     * @param tablePath  table路径
     * @param colName    列名
     * @param fileId     fileId
     * @param offset     页偏移
     * @return
     */
    IndexPage readIndex(String tablePath, String colName, short fileId, int offset) throws PersistenceException;

    LeafIndexPage readLeafIndex(String tablePath, String colName, short fileId, int offset) throws PersistenceException;

    /**
     * 读取第一个文件的一个数据页
     * @param tablePath table路径
     * @param offset    偏移量
     * @return
     */
    default DataPage readPage(String tablePath, int offset) throws PersistenceException {
        return readPage(tablePath, StorageConst.TABLE_DEF_FILE_ID, offset);
    }

    /**
     * 读取第一个文件的一个数据页，默认offset为0
     * @param tablePath table路径
     * @return
     */
    default DataPage readPage(String tablePath) throws PersistenceException {
        return readPage(tablePath,0);
    }

    default IndexPage readIndex(String tablePath, String colName) throws PersistenceException {
        return readIndex(tablePath, colName, StorageConst.INDEX_DEF_FILE_ID);
    }

    default IndexPage readIndex(String tablePath, String colName, short fileId) throws PersistenceException {
        return readIndex(tablePath, colName, fileId, 0);
    }

    /**
     *
     * @param paths 各个域的名称
     * @return storagePath
     */
    String resolvePath(Object... paths);

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

    void move(String fromPath, String toPath) throws PersistenceException;

    void writeStatistics(String tablePath, Map<String, Object> data) throws PersistenceException;

    Map<String, Object> readStatistics(String tablePath) throws PersistenceException;
}
