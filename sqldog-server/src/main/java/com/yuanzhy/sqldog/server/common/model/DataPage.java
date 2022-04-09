package com.yuanzhy.sqldog.server.common.model;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/9
 */
public class DataPage {
    /** 存储标识 */
    private final String fileId;
    /** page offset */
    private final int offset;
    /** 页数据 16K */
    private final byte[] data;

    public DataPage(String fileId, byte[] data) {
        this(fileId, 0, data);
    }

    public DataPage(String fileId, int offset, byte[] data) {
        this.fileId = fileId;
        this.offset = offset;
        this.data = data;
    }

    public String getFileId() {
        return fileId;
    }

    public int getOffset() {
        return offset;
    }

    public byte[] getData() {
        return data;
    }
}