package com.yuanzhy.sqldog.server.common.model;

import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.core.util.ByteUtil;

/**
 * 数据页 = 16KB
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/9
 */
public class DataPage extends Page {
    /** 定位一条记录 */
    private Location location;

    public DataPage(short fileId) {
        this(fileId, 0, new byte[StorageConst.PAGE_SIZE]);
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

    public DataPage(short fileId, byte[] data) {
        this(fileId, 0, data);
    }

    public DataPage(short fileId, int offset, byte[] data) {
        super(fileId, offset, data);
    }

    public Location getLocation() {
        return location;
    }

    public void setLocation(Location location) {
        this.location = location;
    }

    @Override
    public byte[] toAddress() {
        // 数据地址值：8字节，其中2字节表示数据文件id，2字节表示页偏移，2字节表示offset，2字节表示length
        byte[] bytes1 = ByteUtil.toBytes(getFileId()); // 定位到文件
        byte[] bytes2 = ByteUtil.toBytes((short) getOffset()); // 定位到第几页
        byte[] bytes3 = ByteUtil.toBytes(getLocation().getOffset()); // 数据起始偏移
        byte[] bytes4 = ByteUtil.toBytes(getLocation().getLength()); // 数据长度
        return new byte[]{bytes1[0], bytes1[1], bytes2[0], bytes2[1], bytes3[0], bytes3[1], bytes4[0], bytes4[1]};
    }

    public static DataPage fromAddress(byte[] pageBuf, int start) {
        short fileId = ByteUtil.toShort(pageBuf, start);
        start += 2;
        int pageOffset = ByteUtil.toShort(pageBuf, start);
        start += 2;
        short dataOffset = ByteUtil.toShort(pageBuf, start);
        start += 2;
        short dataLength = ByteUtil.toShort(pageBuf, start);
        DataPage dataPage = new DataPage(fileId, pageOffset, null);
        dataPage.setLocation(new Location(dataOffset, dataLength));
        return dataPage;
    }
}