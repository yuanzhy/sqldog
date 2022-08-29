package com.yuanzhy.sqldog.server.common.model;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.util.Arrays;

import com.yuanzhy.sqldog.core.exception.PersistenceException;
import com.yuanzhy.sqldog.core.util.ArrayUtils;
import com.yuanzhy.sqldog.core.util.ByteUtil;
import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.storage.persistence.PersistenceFactory;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/17
 */
public abstract class IndexPage extends Page {

    protected final String columnName;

    public IndexPage(String tablePath, String columnName, short fileId) {
        this(tablePath, columnName, fileId, 0);
    }

    protected IndexPage(String tablePath, String columnName, short fileId, int offset) {
        this(tablePath, columnName, fileId, offset, newLeafBuffer());
    }

    @Deprecated
    public IndexPage(String tablePath, String columnName, short fileId, byte[] data) {
        this(tablePath, columnName, fileId, 0, data);
    }

    protected IndexPage(String tablePath, String columnName, short fileId, int offset, byte[] data) {
        super(tablePath, fileId, offset, data);
        this.columnName = columnName;
    }

    @Override
    public byte[] toAddress() {
        // 索引地址4字节，其中2字节表示索引文件id，2字节表示页偏移
        byte[] bytes1 = ByteUtil.toBytes(getFileId()); // 定位到文件
        byte[] bytes2 = ByteUtil.toBytes((short) getOffset()); // 定位到第几页
        return new byte[]{bytes1[0], bytes1[1], bytes2[0], bytes2[1]};
    }

    @Override
    public abstract IndexPage copyTo(short fileId);
    public boolean isLeaf() {
        return data[8] == 0;
    }

    public boolean isBranch() {
        return !isLeaf();
    }

    public static IndexPage from(RandomAccessFile raf, String tablePath, String colName, short fileId, int offset) {
        byte[] data = new byte[StorageConst.PAGE_SIZE];
        try {
            int n = raf.read(data);
            if (data.length != n) {
                throw new PersistenceException("Illegal extra data, The correct size is " + data.length + ", in fact is " + n);
            }
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
        if (data[8] == 0) {
            return new LeafIndexPage(tablePath, colName, fileId, offset, data);
        } else {
            return new BranchIndexPage(tablePath, colName, fileId, offset, data);
        }
    }

    public static IndexPage to(RandomAccessFile raf, String tablePath, String colName, short fileId, int offset) {
        byte[] data = new byte[StorageConst.PAGE_SIZE];
        try {
            int n = raf.read(data);
            if (data.length != n) {
                throw new PersistenceException("Illegal extra data, The correct size is " + data.length + ", in fact is " + n);
            }
        } catch (IOException e) {
            throw new PersistenceException(e);
        }
        if (data[8] == 0) {
            return new LeafIndexPage(tablePath, colName, fileId, offset, data);
        } else {
            return new BranchIndexPage(tablePath, colName, fileId, offset, data);
        }
    }

    protected static byte[] newLeafBuffer() {
        return newBuffer(0);
    }

    @Deprecated
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

    public byte[] value(final int start) {
        int dataStart = start;
        int valLength = ByteUtil.toShort(data, dataStart);
        dataStart += 2; // 跳过数据长度标志位
        return ArrayUtils.subarray(data, dataStart, dataStart + valLength);
    }

    public abstract byte[] minValue();

    public IndexPage level(int level) {
        data[8] = (byte) level;
        return this;
    }

    public int level() {
        return data[8];
    }

    protected int compare(Column column, byte[] v1, byte[] v2) {
        if (Arrays.equals(v1, v2)) {
            return 0;
        }
        switch (column.getDataType()) {
            case INT:
            case SERIAL:
                return Integer.compare(ByteUtil.toInt(v1), ByteUtil.toInt(v2));
            case BIGINT:
            case BIGSERIAL:
            case DATE:
            case TIMESTAMP:
            case TIME:
                return Long.compare(ByteUtil.toLong(v1), ByteUtil.toLong(v2));
            case SMALLINT:
            case SMALLSERIAL:
                return Short.compare(ByteUtil.toShort(v1), ByteUtil.toShort(v2));
            case TINYINT:
            case BOOLEAN:
                return Byte.compare(v1[0], v2[0]);
            case FLOAT:
                return Float.compare(ByteUtil.toFloat(v1), ByteUtil.toFloat(v2));
            case DOUBLE:
                return Double.compare(ByteUtil.toDouble(v1), ByteUtil.toDouble(v2));
            case BYTEA:
            case TEXT:
            case ARRAY:
            case JSON:
                throw new UnsupportedOperationException("暂未实现大字段比较大小");
            case DECIMAL:
            case NUMERIC:
                return new BigDecimal(ByteUtil.toString(v1)).compareTo(new BigDecimal(ByteUtil.toString(v2)));
            default: // VARCHAR, CHAR
                return ByteUtil.toString(v1).compareTo(ByteUtil.toString(v2));
        }
    }

    public void save() {
        PersistenceFactory.get().writeIndex(tablePath, columnName, this);
    }

    public byte[] address(final int start) {
        return ArrayUtils.subarray(this.data, start, start + 8);
    }

    public class LeafResult {
        public final boolean isNew; // 是否新增，如果有相同的索引值，则为false
        public final int dataStart; // 数据地址的开头，即LEN后面  [值-LEN-多个数据地址值]
        public final LeafIndexPage leafPage;
        public final int addressLength;
//        LeafResult(boolean isNew, int dataStart, LeafIndexPage leafPage) {
//            this(isNew, dataStart, leafPage, -1);
//        }

        LeafResult(int dataStart, LeafIndexPage leafPage, int addrLen) {
            this(false, dataStart, leafPage, addrLen);
        }

        LeafResult(boolean isNew, int dataStart, LeafIndexPage leafPage, int addrLen) {
            this.isNew = isNew;
            this.dataStart = dataStart;
            this.leafPage = leafPage;
            this.addressLength = addrLen;
        }
    }
}
