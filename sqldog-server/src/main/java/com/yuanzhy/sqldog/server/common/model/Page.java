package com.yuanzhy.sqldog.server.common.model;

import com.yuanzhy.sqldog.core.exception.PersistenceException;
import com.yuanzhy.sqldog.core.util.ByteUtil;
import com.yuanzhy.sqldog.server.common.StorageConst;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/17
 */
public abstract class Page {

    protected final String tablePath;
    /** 存储标识 */
    protected final short fileId;
    /** page offset */
    protected final int offset;
    /** 页数据 16K */
    protected final byte[] data;

    public Page(String tablePath, short fileId, int offset, byte[] data) {
        this.tablePath = tablePath;
        this.fileId = fileId;
        this.offset = offset;
        this.data = data;
    }

    public short getFileId() {
        return fileId;
    }

    public int getOffset() {
        return offset;
    }

    public short freeStart() {
        return ByteUtil.toShort(data, StorageConst.FREE_START_OFFSET);
    }

    public short freeEnd() {
        return ByteUtil.toShort(data, StorageConst.FREE_END_OFFSET);
    }

    protected void updateFreeStart(int freeStart) {
        byte[] startBytes = ByteUtil.toBytes((short)freeStart);
        data[StorageConst.FREE_START_OFFSET] = startBytes[0];
        data[StorageConst.FREE_START_OFFSET+1] = startBytes[1];
    }

    public abstract byte[] toAddress();

    public abstract Page copyTo(short fileId);

    public void fillDataFrom(RandomAccessFile raf) {
        try {
            int n = raf.read(data);
            if (data.length != n) {
                throw new PersistenceException("Illegal extra data, The correct size is " + data.length + ", in fact is " + n);
            }
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }

    public void dumpDataTo(RandomAccessFile raf) {
        try {
            raf.write(this.data);
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
    }
}
