package com.yuanzhy.sqldog.server.common.model;

/**
 * 待更新的索引信息
 * 用于缓存树枝索引，等叶子索引更新后在根据情况去依次更新树枝索引
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/18
 */
public class UpdatedIndex {
    /** 待更新的索引页 */
    private final IndexPage indexPage;
    /** 待更新的起始位置 - 字节数 */
    private final int dataStart;
    /** 待更新的值 */
    private final byte[] value;

    public UpdatedIndex(IndexPage indexPage, int dataStart, byte[] value) {
        this.indexPage = indexPage;
        this.dataStart = dataStart;
        this.value = value;
    }

    public IndexPage getIndexPage() {
        return indexPage;
    }

    public int getDataStart() {
        return dataStart;
    }

    public byte[] getValue() {
        return value;
    }
}
