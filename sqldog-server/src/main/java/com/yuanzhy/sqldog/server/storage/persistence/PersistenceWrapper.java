package com.yuanzhy.sqldog.server.storage.persistence;

import com.yuanzhy.sqldog.core.exception.PersistenceException;
import com.yuanzhy.sqldog.server.common.model.DataExtent;
import com.yuanzhy.sqldog.server.common.model.DataPage;
import com.yuanzhy.sqldog.server.common.model.IndexPage;
import com.yuanzhy.sqldog.server.common.model.LeafIndexPage;
import com.yuanzhy.sqldog.server.core.Persistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/5/14
 */
public class PersistenceWrapper implements Persistence {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    protected final Persistence delegate;

    PersistenceWrapper(Persistence persistence) {
        this.delegate = persistence;
    }
    @Override
    public Map<String, Object> readMeta(String storagePath) throws PersistenceException {
        return delegate.readMeta(storagePath);
    }

    @Override
    public void writeMeta(String storagePath, Map<String, Object> data) throws PersistenceException {
        delegate.writeMeta(storagePath, data);
    }

    @Override
    public void delete(String storagePath) throws PersistenceException {
        delegate.delete(storagePath);
    }

    @Override
    public List<String> list(String storagePath) throws PersistenceException {
        return delegate.list(storagePath);
    }

    @Override
    public DataPage readPage(String tablePath, short fileId, int offset) throws PersistenceException {
        return delegate.readPage(tablePath, fileId, offset);
    }

    @Override
    public DataExtent readExtent(String tablePath, short fileId, int offset) throws PersistenceException {
        return delegate.readExtent(tablePath, fileId, offset);
    }

    @Override
    public DataPage getInsertablePage(String tablePath) throws PersistenceException {
        return delegate.getInsertablePage(tablePath);
    }

    @Override
    public DataPage writePage(String tablePath, DataPage dataPage) throws PersistenceException {
        return delegate.writePage(tablePath, dataPage);
    }

    @Override
    public byte[] readExtraData(String tablePath, byte[] extraId) throws PersistenceException {
        return delegate.readExtraData(tablePath, extraId);
    }

    @Override
    public byte[] writeExtraData(String tablePath, byte[] bytes) throws PersistenceException {
        return delegate.writeExtraData(tablePath, bytes);
    }

    @Override
    public void writeIndex(String tablePath, String colName, IndexPage indexPage) throws PersistenceException {
        delegate.writeIndex(tablePath, colName, indexPage);
    }

    @Override
    public IndexPage newIndex(String tablePath, String colName, int level) throws PersistenceException {
        return delegate.newIndex(tablePath, colName, level);
    }

    @Override
    public IndexPage getInsertableIndex(String tablePath, String colName, int level) throws PersistenceException {
        return delegate.getInsertableIndex(tablePath, colName, level);
    }

    @Override
    public IndexPage readIndex(String tablePath, String colName, short fileId, int offset) throws PersistenceException {
        return delegate.readIndex(tablePath, colName, fileId, offset);
    }

    @Override
    public LeafIndexPage readLeafIndex(String tablePath, String colName, short fileId, int offset) throws PersistenceException {
        return delegate.readLeafIndex(tablePath, colName, fileId, offset);
    }

    @Override
    public String resolvePath(Object... paths) {
        return delegate.resolvePath(paths);
    }

    @Override
    public void move(String fromPath, String toPath) throws PersistenceException {
        delegate.move(fromPath, toPath);
    }

    @Override
    public void writeStatistics(String tablePath, Map<String, Object> data) throws PersistenceException {
        delegate.writeStatistics(tablePath, data);
    }

    @Override
    public Map<String, Object> readStatistics(String tablePath) throws PersistenceException {
        return delegate.readStatistics(tablePath);
    }
}
