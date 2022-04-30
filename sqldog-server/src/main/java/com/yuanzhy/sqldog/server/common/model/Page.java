package com.yuanzhy.sqldog.server.common.model;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/17
 */
public abstract class Page {

    /** 存储标识 */
    protected short fileId;
    /** page offset */
    protected int offset;
    /** 页数据 16K */
    protected final byte[] data;

    public Page(short fileId, int offset, byte[] data) {
        this.fileId = fileId;
        this.offset = offset;
        this.data = data;
    }

    public short getFileId() {
        return fileId;
    }

    public void setFileId(short fileId) {
        this.fileId = fileId;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public byte[] getData() {
        return data;
    }

    public abstract byte[] toAddress();
}
