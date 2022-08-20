package com.yuanzhy.sqldog.server.common.model;

import com.yuanzhy.sqldog.core.util.ByteUtil;
import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.storage.persistence.PersistenceFactory;
import org.apache.commons.lang3.ArrayUtils;

import java.math.BigDecimal;
import java.util.Arrays;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/17
 */
public class IndexPage extends Page {

    protected final String columnName;

    public IndexPage(String tablePath, String columnName, short fileId) {
        this(tablePath, columnName, fileId, 0);
    }

    public IndexPage(String tablePath, String columnName, short fileId, int offset) {
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
    public IndexPage copyTo(short fileId) {
        return new IndexPage(tablePath, columnName, fileId, 0, data);
    }

    public boolean isLeaf() {
        return data[8] == 0;
    }

    @Deprecated
    public static IndexPage fromAddress(IndexPage indexPage, int start) {
        short fileId = ByteUtil.toShort(indexPage.data, start);
        start += 2;
        int pageOffset = ByteUtil.toShort(indexPage.data, start);
        return new IndexPage(indexPage.tablePath, indexPage.columnName, fileId, pageOffset, null);
    }

    protected byte[] newBuffer() {
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
        buf[8] = (byte) 0;
        return buf;
    }

    @Deprecated
    public static byte[] newLeafBuffer() {
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

    public byte[] val(final int start) {
        int dataStart = start;
        int valLength = ByteUtil.toShort(data, dataStart);
        dataStart += 2; // 跳过数据长度标志位
        return ArrayUtils.subarray(data, dataStart, dataStart + valLength);
    }

    public byte[] minValue() { // 获取索引页的最小值
        short valLen = ByteUtil.toShort(data, StorageConst.INDEX_LEAF_START);
        return ArrayUtils.subarray(data, StorageConst.INDEX_LEAF_START + 2, StorageConst.INDEX_LEAF_START + 2 + valLen);
    }

    public void level(int level) {
        data[8] = (byte) level;
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

    public int addIndexValue(LeafIndexPage leafPage, byte[] value, int dataStart) {
        // 非叶子节点不含 PREV_PAGE and NEXT_PAGE
        // 写入索引数据：[值-索引地址值]
        byte[] valuelen = ByteUtil.toBytes((short)value.length);
        System.arraycopy(valuelen, 0, data, dataStart, 2);
        dataStart += 2;
        for (byte v : value) {
            data[dataStart++] = v;
        }
        // 写入 索引地址值
        // 索引地址值：4字节，其中2字节表示索引文件id，2字节表示页偏移
        for (byte b : leafPage.toAddress()) {
            data[dataStart++] = b;
        }
        updateFreeStart(dataStart);
        return dataStart;
    }

    public int addIndexValue(LeafIndexPage leafPage, byte[] value) {
        return addIndexValue(leafPage, value, StorageConst.INDEX_BRANCH_START);
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
