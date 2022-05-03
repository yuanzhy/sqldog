package com.yuanzhy.sqldog.server.common.model;

import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.core.util.ByteUtil;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/17
 */
public class IndexPage extends Page {


    public IndexPage(short fileId, byte[] data) {
        this(fileId, 0, data);
    }

    public IndexPage(short fileId, int offset, byte[] data) {
        super(fileId, offset, data);
    }

    @Override
    public byte[] toAddress() {
        // 索引地址4字节，其中2字节表示索引文件id，2字节表示页偏移
        byte[] bytes1 = ByteUtil.toBytes(getFileId()); // 定位到文件
        byte[] bytes2 = ByteUtil.toBytes((short) getOffset()); // 定位到第几页
        return new byte[]{bytes1[0], bytes1[1], bytes2[0], bytes2[1]};
    }

    public static IndexPage fromAddress(byte[] pageBuf, int start) {
        short fileId = ByteUtil.toShort(pageBuf, start);
        start += 2;
        int pageOffset = ByteUtil.toShort(pageBuf, start);
        return new IndexPage(fileId, pageOffset, null);
    }

    public static byte[] newLeafBuffer() {
        return newBuffer(0);
    }

    public static byte[] newBuffer(int level) {
        // 先写入叶子节点
        byte[] buf = new byte[StorageConst.PAGE_SIZE];
        // Page Header
        //  - CHECKSUM 未实现
//            leafPage[0] = leafPage[1] = leafPage[2] = leafPage[3] = 0;
        //  - FREE_START  FREE_END
        //  - FREE_START  FREE_END
        byte[] endBytes = ByteUtil.toBytes(StorageConst.PAGE_SIZE);
        buf[6] = endBytes[0];
        buf[7] = endBytes[1];
        buf[8] = (byte)level;
        return buf;
    }
}
