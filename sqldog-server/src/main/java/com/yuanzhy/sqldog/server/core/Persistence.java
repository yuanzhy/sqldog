package com.yuanzhy.sqldog.server.core;

import com.yuanzhy.sqldog.core.exception.PersistenceException;
import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.common.model.DataExtent;
import com.yuanzhy.sqldog.server.common.model.DataPage;
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
     * 读取一个数据页
     * @param tablePath table路径
     * @param pageId    文件标识
     * @param offset    偏移量
     * @return DataPage: nullable
     */
    DataPage readPage(String tablePath, String pageId, int offset) throws PersistenceException;

    /**
     * 读取一个数据区
     * @param tablePath table路径
     * @param fileId    文件标识
     * @param offset    偏移量
     * @return
     * @throws PersistenceException
     */
    DataExtent readExtent(String tablePath, String fileId, int offset) throws PersistenceException;

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
     * 读取第一个文件的一个数据区
     * @param tablePath table路径
     * @param offset    偏移量
     * @return
     */
    default DataExtent readExtent(String tablePath, int offset) {
        return readExtent(tablePath, StorageConst.TABLE_DEF_FILE_ID, offset);
    }
    /**
     * 读取第一个文件的一个数据区，默认offset为0
     * @param tablePath table路径
     * @return
     */
    default DataExtent readExtent(String tablePath) {
        return readExtent(tablePath, 0);
    }
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

    void move(String fromPath, String toPath) throws PersistenceException;

}
