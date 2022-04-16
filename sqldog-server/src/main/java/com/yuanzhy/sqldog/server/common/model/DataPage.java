package com.yuanzhy.sqldog.server.common.model;

import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.util.ByteUtil;

/**
 * 数据页 = 16KB
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/9
 */
public class DataPage {
    /** 存储标识 */
    private final String pageId;
    /** page offset */
    private final int offset;
    /** 页数据 16K */
    private final byte[] data;

    public DataPage(String pageId) {
        this.pageId = pageId;
        this.offset = 0;
        this.data = new byte[StorageConst.PAGE_SIZE];
        // Page Header
        //  - CHKSUM 未实现
        data[0] = data[1] = data[2] = data[3] = 0;
        //  - FREE_START  FREE_END
        ByteUtil.toBytes(StorageConst.DATA_START_OFFSET);

        byte[] startBytes = ByteUtil.toBytes(StorageConst.DATA_START_OFFSET);
        data[4] = startBytes[0];
        data[5] = startBytes[1];
        byte[] endBytes = ByteUtil.toBytes(StorageConst.PAGE_SIZE);
        data[6] = endBytes[0];
        data[7] = endBytes[1];
    }

    public DataPage(String pageId, byte[] data) {
        this(pageId, 0, data);
    }

    public DataPage(String pageId, int offset, byte[] data) {
        this.pageId = pageId;
        this.offset = offset;
        this.data = data;
    }

    public String getPageId() {
        return pageId;
    }

    public int getOffset() {
        return offset;
    }

    public byte[] getData() {
        return data;
    }
}