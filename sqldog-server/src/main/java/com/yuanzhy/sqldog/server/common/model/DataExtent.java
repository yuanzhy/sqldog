package com.yuanzhy.sqldog.server.common.model;

/**
 * 数据区 = 64 * DataPage = 1MB
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/16
 */
public class DataExtent {

    /** 存储标识 */
    private final String fileId;
    /** extent offset */
    private final int offset;
    /** 数据区 16K*64=1M */
//    private final DataPage[] dataPages;
    private final byte[][] pages;

    public DataExtent(String fileId, int offset, byte[][] pages) {
        this.fileId = fileId;
        this.offset = offset;
        this.pages = pages;
    }

    public String getFileId() {
        return fileId;
    }

    public int getOffset() {
        return offset;
    }

//    public DataPage[] getDataPages() {
//        return dataPages;
//    }

    public byte[] getPage(int pageOffset) {
        if (0 <= pageOffset && pageOffset < pages.length) {
            return pages[pageOffset];
        }
        return null;
    }
}
