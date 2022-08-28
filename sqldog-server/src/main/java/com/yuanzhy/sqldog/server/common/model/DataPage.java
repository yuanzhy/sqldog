package com.yuanzhy.sqldog.server.common.model;

import com.yuanzhy.sqldog.core.util.ByteUtil;
import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.common.collection.PrimitiveByteList;
import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.core.Persistence;
import com.yuanzhy.sqldog.server.core.constant.DataType;
import com.yuanzhy.sqldog.server.storage.persistence.PersistenceFactory;
import org.apache.commons.lang3.ArrayUtils;

import java.math.BigDecimal;
import java.util.Collection;

/**
 * 数据页 = 16KB
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/9
 */
public class DataPage extends Page {
    /** 定位一条记录 */
    private Location location;

    public DataPage(String tablePath, short fileId) {
        this(tablePath, fileId, 0, new byte[StorageConst.PAGE_SIZE]);
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

    public DataPage(String tablePath, short fileId, int offset) {
        this(tablePath, fileId, offset, new byte[StorageConst.PAGE_SIZE]);
    }

    private DataPage(String tablePath, short fileId, int offset, byte[] data) {
        super(tablePath, fileId, offset, data);
    }

    public Location getLocation() {
        return location;
    }

    public void setLocation(Location location) {
        this.location = location;
    }

    @Deprecated
    @Override
    public byte[] toAddress() {
        // 数据地址值：8字节，其中2字节表示数据文件id，2字节表示页偏移，2字节表示offset，2字节表示length
        byte[] bytes1 = ByteUtil.toBytes(getFileId()); // 定位到文件
        byte[] bytes2 = ByteUtil.toBytes((short) getOffset()); // 定位到第几页
        byte[] bytes3 = ByteUtil.toBytes(getLocation().getOffset()); // 数据起始偏移
        byte[] bytes4 = ByteUtil.toBytes(getLocation().getLength()); // 数据长度
        return new byte[]{bytes1[0], bytes1[1], bytes2[0], bytes2[1], bytes3[0], bytes3[1], bytes4[0], bytes4[1]};
    }

    /**
     * 返回一行数据的地址值
     * 此值作为索引，可以指定定位到一条数据的起始位置
     * @param row 数据行
     * @return
     */
    public byte[] toAddress(Row row) {
        // 数据地址值：8字节，其中2字节表示数据文件id，2字节表示页偏移，2字节表示offset，2字节表示length
        byte[] bytes1 = ByteUtil.toBytes(getFileId()); // 定位到文件
        byte[] bytes2 = ByteUtil.toBytes((short) getOffset()); // 定位到第几页
        byte[] bytes3 = ByteUtil.toBytes((short) row.start); // 数据起始偏移
        byte[] bytes4 = ByteUtil.toBytes((short) (row.end - row.start)); // 数据长度
        return new byte[]{bytes1[0], bytes1[1], bytes2[0], bytes2[1], bytes3[0], bytes3[1], bytes4[0], bytes4[1]};
    }


    @Override
    public DataPage copyTo(short fileId) {
        DataPage dataPage = new DataPage(tablePath, fileId, 0, data);
        dataPage.location = this.location;
        return dataPage;
    }

//    public static DataPage fromAddress(byte[] pageBuf, int start) {
//        short fileId = ByteUtil.toShort(pageBuf, start);
//        start += 2;
//        int pageOffset = ByteUtil.toShort(pageBuf, start);
//        start += 2;
//        short dataOffset = ByteUtil.toShort(pageBuf, start);
//        start += 2;
//        short dataLength = ByteUtil.toShort(pageBuf, start);
//        DataPage dataPage = new DataPage(fileId, pageOffset, null);
//        dataPage.setLocation(new Location(dataOffset, dataLength));
//        return dataPage;
//    }

    public DataPage insert(PrimitiveByteList dataBytes) {
        byte[] pageBuf = this.data;
        short freeStart = ByteUtil.toShort(pageBuf, StorageConst.FREE_START_OFFSET);
        short freeEnd = ByteUtil.toShort(pageBuf, StorageConst.FREE_END_OFFSET);
        // ------- 写入数据 -------
        if (dataBytes.size() >= StorageConst.PAGE_SIZE - 16) {
            // TODO
            throw new RuntimeException("单条记录大于一页的情况暂未实现：" + dataBytes.size());
        }
//        Persistence persistence = PersistenceFactory.get();
        if (freeEnd - freeStart < dataBytes.size()) {  // 剩余page空间不够存储本条记录
            // 生成一个新的 page 追加到文件末尾
            byte[] newPageBuf = new byte[StorageConst.PAGE_SIZE];
            Location location = fillPageBuf(dataBytes, newPageBuf, StorageConst.DATA_START_OFFSET, StorageConst.PAGE_SIZE);
            // dataPage.getOffset() + StorageConst.PAGE_SIZE == file.length
            DataPage newDataPage = new DataPage(tablePath, this.getFileId(), this.getOffset() + 1, newPageBuf);
//            newDataPage = persistence.writePage(tablePath, newDataPage);
            newDataPage.setLocation(location);
            return newDataPage;
        } else { // page 空间够用, 在当前 page 空闲区写入数据
            Location location = fillPageBuf(dataBytes, pageBuf, freeStart, freeEnd);
            this.setLocation(location);
            return this;
            // 将当前 page 回写文件
//            DataPage newDataPage = persistence.writePage(tablePath, this);
//            newDataPage.setLocation(location);
//            return newDataPage;
        }
    }

    public DataPage save() {
        Persistence persistence = PersistenceFactory.get();
        return persistence.writePage(tablePath, this);
    }

    private Location fillPageBuf(PrimitiveByteList dataBytes, byte[] pageBuf, short freeStart, short freeEnd) {
        // Page Header
        //  - CHECKSUM 未实现
        pageBuf[0] = pageBuf[1] = pageBuf[2] = pageBuf[3] = 0;
        //  - FREE_START  FREE_END
//        pageBuf[4] = (byte)(freeStart >> 8 & 0xff);
//        pageBuf[5] = (byte)(freeStart & 0xff);
        byte[] endBytes = ByteUtil.toBytes(freeEnd);
        pageBuf[6] = endBytes[0];
        pageBuf[7] = endBytes[1];
        //  - PAGE_MAX_TRX_ID 未实现
        pageBuf[8] = pageBuf[9] = pageBuf[10] = pageBuf[11] = pageBuf[12] = pageBuf[13] = pageBuf[14] = pageBuf[15] = 0;

        // 写入 data (header + data)
        int locStart = freeStart;
        for (int i = 0; i < dataBytes.size(); i++) {
            pageBuf[freeStart++] = dataBytes.get(i);
        }
        // 更新freeStart字段
        byte[] startBytes = ByteUtil.toBytes(freeStart);
        pageBuf[4] = startBytes[0];
        pageBuf[5] = startBytes[1];
        return new Location((short)locStart, (short)(freeStart - locStart));
    }

    public Row row(final Collection<Column> columns, final int nullBytesCount, final int start) {
        int dataStart = start;
        final byte[] pageBuf = this.data;
        short dataHeader = ByteUtil.toShort(pageBuf, dataStart);
        dataStart += 2;
        final int end = dataStart + (dataHeader & 0b0111111111111111);
        boolean deleted = ((dataHeader >> 15) & 0b1) == 1;
        if (deleted) {
            return new Row(start, end, null);
        }
        Object[] row = new Object[columns.size()];
        // 读取nullFlag
        byte[] nullFlags = new byte[nullBytesCount];
//            for (int i = nullBytesCount - 1; i >= 0; i--) {
//                nullFlags[i] = pageBuf[dataStart++];
//            }
        for (int i = 0; i < nullBytesCount; i++) {
            nullFlags[i] = pageBuf[dataStart++];
        }
        // 读数据
        while (dataStart < end) { // 读取一行数据
            int colIdx = 0;
            int nullIdx = 0;
            for (Column column : columns) {
                if (column.isNullable()) {
                    byte nullBit = (byte) Math.pow(2, nullIdx % 8);
                    if (nullBit == (nullFlags[nullIdx / 8] & nullBit)) { // null位为1，说明对应的值为空
                        nullIdx++;
                        colIdx++;
                        continue;
                    }
                    nullIdx++;
                }
                switch (column.getDataType()) {
                    case INT:
                    case SERIAL:
                        row[colIdx++] = ByteUtil.toInt(pageBuf, dataStart);
                        dataStart += 4;
                        break;
                    case BIGINT:
                    case BIGSERIAL:
                        row[colIdx++] = ByteUtil.toLong(pageBuf, dataStart);
                        dataStart += 8;
                        break;
                    case SMALLINT:
                    case SMALLSERIAL:
                        row[colIdx++] = ByteUtil.toShort(pageBuf, dataStart);
                        dataStart += 2;
                        break;
                    case TINYINT:
                        row[colIdx++] = pageBuf[dataStart++];
                        break;
                    case FLOAT:
                        row[colIdx++] = ByteUtil.toFloat(pageBuf, dataStart);
                        dataStart += 4;
                        break;
                    case DOUBLE:
                        row[colIdx++] = ByteUtil.toDouble(pageBuf, dataStart);
                        dataStart += 8;
                        break;
                    case DATE:
                        row[colIdx++] = new java.sql.Date(ByteUtil.toLong(pageBuf, dataStart) + 28800000);
                        dataStart += 8;
                        break;
                    case TIMESTAMP: // TODO calcite 坑，先用野路子处理一下
                        row[colIdx++] = new java.sql.Timestamp(ByteUtil.toLong(pageBuf, dataStart) + 28800000);
                        dataStart += 8;
                        break;
                    case TIME:
                        row[colIdx++] = new java.sql.Time(ByteUtil.toLong(pageBuf, dataStart) + 28800000);
                        dataStart += 8;
                        break;
                    case BOOLEAN:
                        row[colIdx++] = pageBuf[dataStart++] == 1;
                        break;
                    case BYTEA:
                    case TEXT:
                        short fileId = ByteUtil.toShort(pageBuf, dataStart);
                        boolean extra = 0x1000 == (fileId & 0x1000);
                        if (extra) {
                            byte[] extraId = ArrayUtils.subarray(pageBuf, dataStart, dataStart += 8);
                            byte[] bytes = PersistenceFactory.get().readExtraData(tablePath, extraId);
                            row[colIdx++] = column.getDataType() == DataType.TEXT ? ByteUtil.toString(bytes) : bytes;
                        } else {
                            short len = ByteUtil.toShort(pageBuf, dataStart);
                            dataStart += 2;
                            if (column.getDataType() == DataType.TEXT) {
                                row[colIdx++] = ByteUtil.toString(pageBuf, dataStart, len);
                                dataStart += len;
                            } else {
                                row[colIdx++] = ArrayUtils.subarray(pageBuf, dataStart, dataStart += len);
                            }
                        }
                        break;
                    case ARRAY:
                    case JSON:
                        throw new UnsupportedOperationException("暂未实现大字段存储");
                    case DECIMAL:
                    case NUMERIC:
                        int len = ByteUtil.toShort(pageBuf, dataStart);
                        dataStart += 2;
                        row[colIdx++] = new BigDecimal(ByteUtil.toString(pageBuf, dataStart, len));
                        dataStart += len;
                        break;
                    default: // VARCHAR, CHAR
                        len = ByteUtil.toShort(pageBuf, dataStart);
                        dataStart += 2;
                        row[colIdx++] = ByteUtil.toString(pageBuf, dataStart, len);
                        dataStart += len;
                }
            }
        }
        return new Row(start, end, row);
    }

    public void delete(int start) {
        final byte[] pageBuf = this.data;
        short dataHeader = ByteUtil.toShort(pageBuf, start);
        dataHeader |= 0x8000; // 变为删除
        byte[] dataHeaderBytes = ByteUtil.toBytes(dataHeader);
        pageBuf[start] = dataHeaderBytes[0];
        pageBuf[start + 1] = dataHeaderBytes[1];
    }

    /**
     * 替换数据
     * @param start 起始位置
     * @param bytes 数据
     */
    public void replace(int start, PrimitiveByteList bytes) {
        final byte[] pageBuf = this.data;
        for (int i = 0; i < bytes.size(); i++) {
            byte dataByte = bytes.get(i);
            pageBuf[start++] = dataByte;
        }
    }

    public class Row {
        public final int start; // start in data page
        public final int end; // end in data page
        public final Object[] data;
        Row(int start, int end, Object[] data) {
            this.start = start;
            this.end = end;
            this.data = data;
        }
    }
}