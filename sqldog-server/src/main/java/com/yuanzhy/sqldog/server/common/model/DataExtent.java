package com.yuanzhy.sqldog.server.common.model;

/**
 * 数据区 = 64 * DataPage = 1MB
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/16
 */
public class DataExtent {

    /** 存储标识 */
    private final short fileId;
    /** extent offset */
    private final int offset;
    /** 数据区 16K*64=1M */
    private final byte[][] pages;

    public DataExtent(short fileId, int offset, byte[][] pages) {
        this.fileId = fileId;
        this.offset = offset;
        this.pages = pages;
    }

    public short getFileId() {
        return fileId;
    }

    public int getOffset() {
        return offset;
    }

    public byte[] getPage(int pageOffset) {
        if (0 <= pageOffset && pageOffset < pages.length) {
            return pages[pageOffset];
        }
        return null;
    }
}
