package com.yuanzhy.sqldog.server.storage.persistence;

import com.yuanzhy.sqldog.core.constant.Consts;
import com.yuanzhy.sqldog.core.exception.PersistenceException;
import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.common.model.DataPage;
import com.yuanzhy.sqldog.server.common.model.IndexPage;
import com.yuanzhy.sqldog.server.common.model.LeafIndexPage;
import com.yuanzhy.sqldog.server.core.Persistence;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/5/14
 */
public class CachedPersistence extends PersistenceWrapper implements Persistence {
    private static final int SAVE_DELAY = 1000 * 60 * 1;
    private final ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
    /**
     *  key  = tablePath
     * value = DataPage
     */
    private final Map<String, CacheObject<DataPage>> insertableDataPageMap = new ConcurrentHashMap<>();
    /**
     *  key  = tablePath_colName_fileId_offset
     * value = IndexPage
     */
    private final Map<String, CacheObject<IndexPage>> indexPageMap = new ConcurrentHashMap<>();
    /**
     *  key  = tablePath
     * value = statData
     */
    private final Map<String, CacheObject<Map<String, Object>>> statMap = new ConcurrentHashMap<>();

    CachedPersistence(Persistence persistence) {
        super(persistence);
        ses.scheduleAtFixedRate(() -> {
            long now = System.currentTimeMillis();
            List<String> keyList = new ArrayList<>();
            try {
                insertableDataPageMap.forEach((key, cacheObject) -> {
                    if (now - cacheObject.lastModified > SAVE_DELAY) {
                        keyList.add(key);
                    }
                });
                for (String key : keyList) {
                    persistDataCache(key);
                }
                if (!keyList.isEmpty() && insertableDataPageMap.isEmpty()) {
                    logger.info("所有数据缓存均已持久化");
                }
            } catch (Exception e) {
                logger.warn("数据定时任务异常", e);
            }
            try {
                keyList.clear();
                indexPageMap.forEach((key, cacheObject) -> {
                    if (now - cacheObject.lastModified > SAVE_DELAY) {
                        keyList.add(key);
                    }
                });
                for (String key : keyList) {
                    persistIndexCache(key);
                }
                if (!keyList.isEmpty() && indexPageMap.isEmpty()) {
                    logger.info("所有索引缓存均已持久化");
                }
            } catch (Exception e) {
                logger.warn("索引定时任务异常", e);
            }
            try {
                keyList.clear();
                statMap.forEach((key, cacheObject) -> {
                    if (now - cacheObject.lastModified > SAVE_DELAY) {
                        keyList.add(key);
                    }
                });
                for (String key : keyList) {
                    persistStatCache(key);
                }
                if (!keyList.isEmpty() && statMap.isEmpty()) {
                    logger.info("所有统计缓存均已持久化");
                }
            } catch (Exception e) {
                logger.warn("索引定时任务异常", e);
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    private void persistStatCache(String key) {
        CacheObject<Map<String, Object>> cacheObject = statMap.remove(key);
        if (cacheObject == null) {
            logger.error("{} 统计缓存丢失，无法持久化" + key);
        } else {
            logger.info("统计缓存持久化：" + key);
            super.writeStatistics(key, cacheObject.data);
        }
    }

    private void persistIndexCache(String key) {
        CacheObject<IndexPage> cacheObject = indexPageMap.remove(key);
        if (cacheObject == null) {
            logger.error("{} 索引缓存丢失，无法持久化" + key);
        } else {
            logger.info("索引缓存持久化：" + key);
            String[] arr = StringUtils.splitByWholeSeparator(key, Consts.SEPARATOR);
            super.writeIndex(arr[0], arr[1], cacheObject.data);
        }
    }

    private void persistDataCache(String key) {
        CacheObject<DataPage> cacheObject = insertableDataPageMap.remove(key);
        if (cacheObject == null) {
            logger.error("{} 数据缓存丢失，无法持久化" + key);
        } else {
            logger.info("数据缓存持久化：" + key);
            super.writePage(key, cacheObject.data);
        }
    }

    @Override
    public DataPage readPage(String tablePath, short fileId, int offset) throws PersistenceException {
        CacheObject<DataPage> cacheObject = insertableDataPageMap.get(tablePath);
        if (cacheObject != null && cacheObject.data.getFileId() == fileId && cacheObject.data.getOffset() == offset) {
            return cacheObject.data;
        }
        return super.readPage(tablePath, fileId, offset);
    }

    @Override
    public DataPage getInsertablePage(String tablePath) throws PersistenceException {
        CacheObject<DataPage> cacheObject = insertableDataPageMap.get(tablePath);
        if (cacheObject == null) {
            cacheObject = new CacheObject(super.getInsertablePage(tablePath));
            insertableDataPageMap.put(tablePath, cacheObject);
        } else {
            cacheObject.lastModified = System.currentTimeMillis();
            logger.debug("命中数据缓存：{}", tablePath);
        }
        return cacheObject.data;
    }

    @Override
    public DataPage writePage(String tablePath, DataPage dataPage) throws PersistenceException {
        CacheObject<DataPage> cacheObject = insertableDataPageMap.get(tablePath);
        if (cacheObject != null) {
            if (cacheObject.data == dataPage) { // 如果是和缓存中的一致，则先不写入存储
                cacheObject.lastModified = System.currentTimeMillis();
                return dataPage;
            }
            if (dataPage.getFileId() == cacheObject.data.getFileId()
                    && dataPage.getOffset() == cacheObject.data.getOffset()) {
                throw new RuntimeException("数据异常");
            }
            if (dataPage.getFileId() == cacheObject.data.getFileId()
                    && dataPage.getOffset() - cacheObject.data.getOffset() == 1) {
                // 新写入的一页比insertable大，说明是当前insertable的位置不够了，新开了一页。这个时候需要替换insertablePageCache
                // 持久化旧的insertablePage
                super.writePage(tablePath, cacheObject.data);
                insertableDataPageMap.remove(tablePath);
            }
        }
        return super.writePage(tablePath, dataPage);
    }

    @Override
    public IndexPage readIndex(String tablePath, String colName, short fileId, int offset) throws PersistenceException {
        String key = indexKey(tablePath, colName, fileId, offset);
        CacheObject<IndexPage> cacheObject = indexPageMap.get(key);
        if (cacheObject == null) {
            IndexPage indexPage = super.readIndex(tablePath, colName, fileId, offset);
            if (indexPage == null) {
                return null;
            }
            cacheObject = new CacheObject(indexPage);
            indexPageMap.put(key, cacheObject);
        } else {
            cacheObject.lastModified = System.currentTimeMillis();
            logger.debug("命中索引缓存：{}", key);
        }
        return cacheObject.data;
    }

    @Override
    public LeafIndexPage readLeafIndex(String tablePath, String colName, short fileId, int offset) throws PersistenceException {
        String key = indexKey(tablePath, colName, fileId, offset);
        CacheObject<IndexPage> cacheObject = indexPageMap.get(key);
        if (cacheObject == null) {
            LeafIndexPage indexPage = super.readLeafIndex(tablePath, colName, fileId, offset);
            if (indexPage == null) {
                return null;
            }
            cacheObject = new CacheObject(indexPage);
            indexPageMap.put(key, cacheObject);
        } else {
            cacheObject.lastModified = System.currentTimeMillis();
            logger.debug("命中索引缓存：{}", key);
        }
        return (LeafIndexPage)cacheObject.data;
    }

    @Override
    public IndexPage writeIndex(String tablePath, String colName, byte[] newBuf) throws PersistenceException {
        // 如果是写入新页面，则尝试持久化所有缓存避免不一致问题
        final String indexKeyPrefix = tablePath + Consts.SEPARATOR + colName + Consts.SEPARATOR;
        String[] keyArr = indexPageMap.keySet().stream().filter(k -> k.startsWith(indexKeyPrefix)).toArray(String[]::new);
        for (String key : keyArr) {
            persistIndexCache(key);
        }
        return super.writeIndex(tablePath, colName, newBuf);
    }

//    @Override
//    public IndexPage getInsertableIndex(String tablePath, String colName, int level) throws PersistenceException {
//        // TODO cache实现
//        return super.getInsertableIndex(tablePath, colName, level);
//    }

    @Override
    public void writeIndex(String tablePath, String colName, IndexPage indexPage) throws PersistenceException {
        String key = indexKey(tablePath, colName, indexPage.getFileId(), indexPage.getOffset());
        CacheObject<IndexPage> cacheObject = indexPageMap.get(key);
        if (cacheObject != null) {
            if (cacheObject.data != indexPage) { // 如果是和缓存中的一致，则说明哪里出现了问题
                throw new RuntimeException("索引缓存不一致：" + key);
            }
            cacheObject.lastModified = System.currentTimeMillis();
            return; // 是同一个对象就不需要在处理了
        }
        // 缓存中没有，那就直接写入存储吧
        super.writeIndex(tablePath, colName, indexPage);
    }

    @Override
    public void delete(String storagePath) throws PersistenceException {
        tryRemoveCache(storagePath);
        super.delete(storagePath);
    }

    @Override
    public void move(String fromPath, String toPath) throws PersistenceException {
        tryPersistCache(fromPath);
        super.move(fromPath, toPath);
    }

    private void tryPersistCache(String storagePath) {
        // TODO 此处处理的不太好
        if (storagePath.endsWith(StorageConst.TABLE_DATA_PATH)) {
            String tablePath = StringUtils.substringBeforeLast(storagePath, "/");
            if (insertableDataPageMap.containsKey(tablePath)) {
                persistDataCache(tablePath);
            }
        } else if (storagePath.endsWith(StorageConst.TABLE_INDEX_PATH)) {
            String tablePath = StringUtils.substringBeforeLast(storagePath, "/");
            persistIndexCacheByTable(tablePath);
        } else {
            final String possibleTablePath = storagePath;
            if (insertableDataPageMap.containsKey(possibleTablePath)) {
                persistDataCache(possibleTablePath);
            }
            persistIndexCacheByTable(possibleTablePath);
        }
    }

    private void persistIndexCacheByTable(String tablePath) {
        final String indexKeyPrefix = tablePath.concat(Consts.SEPARATOR);
        String[] keyArr = indexPageMap.keySet().stream().filter(k -> k.startsWith(indexKeyPrefix)).toArray(String[]::new);
        for (String key : keyArr) {
            persistIndexCache(key);
        }
    }

    private void tryRemoveCache(String storagePath) {
        // TODO 此处处理的不太好
        if (storagePath.endsWith(StorageConst.TABLE_DATA_PATH)) {
            String tablePath = StringUtils.substringBeforeLast(storagePath, "/");
            insertableDataPageMap.remove(tablePath);
            statMap.remove(tablePath);
        } else if (storagePath.endsWith(StorageConst.TABLE_INDEX_PATH)) {
            String tablePath = StringUtils.substringBeforeLast(storagePath, "/");
            final String indexKeyPrefix = tablePath.concat(Consts.SEPARATOR);
            String[] keyArr = indexPageMap.keySet().stream().filter(k -> k.startsWith(indexKeyPrefix)).toArray(String[]::new);
            for (String key : keyArr) {
                indexPageMap.remove(key);
            }
        } else {
            final String possibleTablePath = storagePath;
            insertableDataPageMap.remove(possibleTablePath);
            statMap.remove(possibleTablePath);
            final String indexKeyPrefix = possibleTablePath.concat(Consts.SEPARATOR);
            String[] keyArr = indexPageMap.keySet().stream().filter(k -> k.startsWith(indexKeyPrefix)).toArray(String[]::new);
            for (String key : keyArr) {
                indexPageMap.remove(key);
            }
        }
    }

//    @Override
//    public Map<String, Object> readStatistics(String tablePath) throws PersistenceException {
//        CacheObject<Map<String, Object>> cacheObject = statMap.get(tablePath);
//        if (cacheObject != null) {
//            cacheObject.lastModified = System.currentTimeMillis();
//            return cacheObject.data;
//        }
//        return super.readStatistics(tablePath);
//    }
    @Override
    public void writeStatistics(String tablePath, Map<String, Object> data) throws PersistenceException {
        CacheObject<Map<String, Object>> cacheObject = statMap.get(tablePath);
        if (cacheObject != null) {
            logger.debug("命中统计缓存: {}", tablePath);
        }
        cacheObject = new CacheObject<>(data);
        statMap.put(tablePath, cacheObject);
    }

    private String indexKey(String tablePath, String colName, short fileId, int offset) {
        return tablePath + Consts.SEPARATOR + colName + Consts.SEPARATOR + fileId + Consts.SEPARATOR + offset;
    }

    private class CacheObject<T> {
        long lastModified;
        final T data;

        CacheObject(T data) {
            this.data = data;
            this.lastModified = System.currentTimeMillis();
        }
    }
}
