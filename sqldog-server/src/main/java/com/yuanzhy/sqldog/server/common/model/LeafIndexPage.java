package com.yuanzhy.sqldog.server.common.model;

import com.yuanzhy.sqldog.core.util.ByteUtil;
import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.core.Persistence;
import com.yuanzhy.sqldog.server.storage.persistence.PersistenceFactory;
import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/8/14
 */
public class LeafIndexPage extends IndexPage {

    public LeafIndexPage(String tablePath, String columnName, short fileId) {
        this(tablePath, columnName, fileId, 0);
    }

    public LeafIndexPage(String tablePath, String columnName, short fileId, int offset) {
        this(tablePath, columnName, fileId, offset, newLeafBuffer());
    }

    LeafIndexPage(String tablePath, String columnName, short fileId, int offset, byte[] data) {
        super(tablePath, columnName, fileId, offset, data);
    }

    public LeafIndexPage(IndexPage indexPage) {
        this(indexPage.tablePath, indexPage.columnName, indexPage.fileId, indexPage.offset, indexPage.data);
    }

    @Override
    public LeafIndexPage copyTo(short fileId) {
        return new LeafIndexPage(tablePath, columnName, fileId, 0, data);
    }

    @Override
    public byte[] minValue() { // 获取索引页的最小值
        return this.value(StorageConst.INDEX_LEAF_START);
    }

    public LeafIndexPage prev() {
        int start = StorageConst.INDEX_LEAF_PREV_PAGE;
        short fileId = ByteUtil.toShort(data, start);
        start += 2;
        int pageOffset = ByteUtil.toShort(data, start);
        // TODO 看看可不可以直接读取为 LeafIndex
        return PersistenceFactory.get().readLeafIndex(tablePath, columnName, fileId, pageOffset);
    }

    public LeafIndexPage next() {
        int start = StorageConst.INDEX_LEAF_NEXT_PAGE;
        short fileId = ByteUtil.toShort(data, start);
        start += 2;
        int pageOffset = ByteUtil.toShort(data, start);
        return PersistenceFactory.get().readLeafIndex(tablePath, columnName, fileId, pageOffset);
    }

    /**
     *
     * @param dataAddress  数据地址值（可以定位到一行记录）
     * @param addressStart 查找的起始位置（LEN之后 数据地址之前）
     * @return
     */
    public LeafResult findAddressStart(final byte[] dataAddress, final int addressStart) {
        int dataStart = addressStart;
        int freeStart = this.freeStart();
        LeafIndexPage leafPage = this;
        final int addrLen = addressLength(dataStart - 4);
        int loop = 0;
        // 没有跳页
        byte[] dataAddr2;
        do {
            if (loop++ >= addrLen) {
                leafPage = null; // not found
                break; // 遍历到头了还没找到
            }
            if (dataStart >= freeStart) {
                leafPage = leafPage.next();
                freeStart = leafPage.freeStart();
                dataStart = StorageConst.INDEX_LEAF_START; // next page的开头直接存储地址值
            }
            dataAddr2 = ArrayUtils.subarray(data, dataStart, dataStart += 8);
        } while (!Arrays.equals(dataAddress, dataAddr2));
        // dataStart - 8 : dataStart对应地址值后面，移动到前面
        return new LeafResult(dataStart, leafPage, addrLen);
    }

    public LeafResult findLeafStart(Column column, byte[] value) {
        final byte[] leafBuf = this.data;
        int dataStart = StorageConst.INDEX_LEAF_START;
        int freeStart = freeStart();
        if (dataStart == freeStart) {
            return new LeafResult(true, dataStart, this, 0);
        }
        byte[] existsVal = value(dataStart);
        int valLength = existsVal.length;
        dataStart += 2 + valLength;
        int compared = compare(column, value, existsVal);
        if (compared == 0) {
            // 和最小值相等，叶子长度+1
//            int dataAddrLen = ByteUtil.toInt(leafBuf, dataStart);
//            dataStart += 4 + 8 * dataAddrLen; //
//            IndexPage nextPage = null;
//            while (dataStart > StorageConst.PAGE_SIZE) {
//                nextPage = IndexPage.fromAddress(leafBuf, StorageConst.INDEX_LEAF_NEXT_PAGE);
//                nextPage = persistence.readIndex(tablePath, column.getName(), nextPage.getFileId(), nextPage.getOffset());
//                dataStart -= StorageConst.PAGE_SIZE;
//            }
            final int addrLen = addressLength(dataStart);
            dataStart += 4; // 跳过地址长度，定位到数据地址的开头
            return new LeafResult(false, dataStart, this, addrLen);
        } else if (compared < 0) {
            // 比最小值还小，直接插入最左边.
            dataStart -= 2 + valLength; // 指针移动到上一条记录的最后
//                    if (dataStart == StorageConst.INDEX_LEAF_START) { // 前面已经没有数据了，直接取第一条数据对应的索引地址
//                        dataStart += 2 + valLength;
//                    } else {
//                        dataStart -= 4; // 获取前一个值对应的索引地址
//                    }
            return new LeafResult(true, dataStart, this, 0);
        } else { // > 0
            boolean found = true;
            LeafIndexPage nextPage = this;
            do {
                int dataAddrLen = addressLength(dataStart);
                dataStart += 4 + 8 * dataAddrLen;  // 跳过地址值数量
                if (dataStart > freeStart) {
                    // TODO 不算数据异常，可能跳页了， 需要从下一页继续, 这块需要测测
                    while (dataStart > StorageConst.PAGE_SIZE) {
                        int _start = StorageConst.INDEX_LEAF_NEXT_PAGE;
                        short fileId = ByteUtil.toShort(nextPage.data, _start);
                        _start += 2;
                        int pageOffset = ByteUtil.toShort(nextPage.data, _start);
                        nextPage = PersistenceFactory.get().readLeafIndex(tablePath, column.getName(), fileId, pageOffset);
                        dataStart -= StorageConst.PAGE_SIZE;
                    }
                    if (nextPage == this) {
                        throw new RuntimeException("数据异常");
                    }
//                    leafBuf = nextPage.getData();
                    freeStart = nextPage.freeStart();
                }
                if (dataStart == freeStart) {
                    found = false;
                    break;
                }
                valLength = ByteUtil.toShort(leafBuf, dataStart);
                dataStart += 2;
                existsVal = ArrayUtils.subarray(leafBuf, dataStart, dataStart += valLength);
            } while (compare(column, value, existsVal) > 0);
            // 跳出循环了， 说明小于等于前一个值
            if (found) { // 获取前一个值的最后
                dataStart -= 2 + valLength; // 找到比插入的值大的了，这时寻找前一个值的地址
            } // else { // 遍历到最后还没有，说明新插入的值是最大的，直接取最后的位置，即dataStart == freeStart}
            if (compare(column, value, existsVal) == 0) {
                // 等值的情况，定位到数据地址值开头，不等值的情况定位再数据地址的末尾 ==========================
                return new LeafResult(false, dataStart + 2 + valLength + 4, nextPage, addressLength(dataStart + 2 + valLength));
            }
            return new LeafResult(true, dataStart, nextPage, 0);
        }
//        return dataStart;
    }

    /**
     * 删除一条数据对应的索引<br>
     * 如果该值对应的索引没有了，连同值一起删除掉
     *
     * @param value
     * @param addressStart 地址值开头
     * @return 是否是最小值（如果索引页最小值变了，需要同时更新树枝索引）
     */
    public boolean deleteOne(final byte[] value, final int addressStart, final int addressLength) {
        final int freeStart = this.freeStart();
        int _len;
        boolean needUpdateBranch = false;
        if (addressLength == 1) { // 只有一个长度, 需要连数据也一起删掉
            _len = 8 + 4 + value.length + 2; // 一条索引数据的总长度(不算索引地址的长度)
            if (addressStart - _len == StorageConst.INDEX_LEAF_START) {
                needUpdateBranch = true;
                // 是最小值了，需要同时删除树枝索引的最小值
            }
        } else {
            _len = 8;
        }
        // 从索引中删除该地址
        System.arraycopy(this.data, addressStart, this.data, addressStart - _len, freeStart - addressStart);
        updateFreeStart(freeStart - _len);
        this.save();
        return needUpdateBranch;
    }

    public void replaceIndexValue(final DataPage dataPage, int dataStart) {
        for (byte b : dataPage.toAddress()) {
            data[dataStart++] = b;
        }
    }

    public void addIndexValue(DataPage dataPage, byte[] value, int dataStart) {
        //  - PREV_PAGE and NEXT_PAGE is 0 表示无
        // 写入索引数据：[值-LENGTH-数据地址值]
        //  索引地址值：4字节，其中2字节表示索引文件id，2字节表示页偏移
        // 上一页下一页地址为0，需要空出16字节

        //   写入长度和值
        byte[] valuelen = ByteUtil.toBytes((short) value.length);
        System.arraycopy(valuelen, 0, data, dataStart, 2);
        dataStart += 2;
        for (byte v : value) {
            data[dataStart++] = v;
        }
        //     写入 LEN
        byte[] addrLen = ByteUtil.toBytes(1); // 首次插入值，LEN为1，用4字节表示
        for (byte v : addrLen) {
            data[dataStart++] = v;
        }
        //     写入 数据地址值
        // 数据地址值：8字节，其中2字节表示数据文件id，2字节表示页偏移，2字节表示offset，2字节表示length
        for (byte b : dataPage.toAddress()) {
            data[dataStart++] = b;
        }
        updateFreeStart(dataStart);
//        return dataStart;
    }

    public void addIndexValue(DataPage dataPage, byte[] value) {
        addIndexValue(dataPage, value, StorageConst.INDEX_LEAF_START); // 叶子节点起始
    }

    public InsertAddressResult addExistingAddressAndSave(final DataPage dataPage, final int dataStart) {
        final int freeStart = this.freeStart();
        final int freeEnd = this.freeEnd();
        if (freeEnd - freeStart >= 8) { // 重复的情况只要能放入一个数据地址值就可以了
            this.insertAddress(dataPage, dataStart);
            return InsertAddressResult.EMPTY;
        } else { // 8字节的地址值也放不下了，直接分裂吧
            return this.insertAddressNewly(dataPage, dataStart);
        }
    }

    private int addressLength(final int start) {
        return ByteUtil.toInt(data, start);
    }

    private InsertAddressResult insertAddressNewly(DataPage dataPage, int dataStart) {
        // TODO 测一测等值的情况
        final Persistence persistence = PersistenceFactory.get();
        final int freeStart = this.freeStart();
        final int freeEnd = this.freeEnd();
        final int dataStartOrigin = dataStart;
        // dataStart后的数据后移
        final LeafIndexPage leafPage2 = persistence.getInsertableLeafIndex(tablePath, columnName);
        final byte[] buf2 = leafPage2.data;
        int updateBranchType = 0;
        // 拷贝1后面的数据到2
        System.arraycopy(this.data, dataStartOrigin, buf2, StorageConst.INDEX_LEAF_START, freeStart - dataStartOrigin);
        int freeStart1 = dataStartOrigin;
        int freeStart2 = StorageConst.INDEX_LEAF_START + freeStart - dataStartOrigin;
        // 判断够不够写入的
        if (8 <= freeEnd - freeStart1) { // 1够,写入1末尾
            for (byte b : dataPage.toAddress()) {
                this.data[freeStart1++] = b;
            }
            updateBranchType = 2; // ----- 写入1末尾的情况 需要更新2页的父索引
        } else if (8 <= freeEnd - freeStart2) { // 2够,写入2开头
            System.arraycopy(buf2, StorageConst.INDEX_LEAF_START, buf2, StorageConst.INDEX_LEAF_START + 8, freeStart - dataStartOrigin);
            // 写入地址值
            System.arraycopy(dataPage.toAddress(), 0, buf2, StorageConst.INDEX_LEAF_START, 8);
            freeStart2 += 8;
        } else {
            throw new RuntimeException("这种情况应该不存在");
        }
        // 更正1和2的头部
        this.updateFreeStart(freeStart1);
        leafPage2.updateFreeStart(freeStart2);
        // 更正2的prev为1，next为1的next
        System.arraycopy(this.toAddress(), 0, buf2, StorageConst.INDEX_LEAF_PREV_PAGE, 4); // 2 prev
        System.arraycopy(this.data, StorageConst.INDEX_LEAF_NEXT_PAGE, buf2, StorageConst.INDEX_LEAF_NEXT_PAGE, 4); // 2 next
        leafPage2.save();
        // 更正1的next为2
        System.arraycopy(leafPage2.toAddress(), 0, this.data, StorageConst.INDEX_LEAF_NEXT_PAGE, 4); // 1 next
        this.save();
        return new InsertAddressResult(updateBranchType, leafPage2);
    }

    /**
     * 再已有索引数据中插入一个地址值
     * @param dataPage dataPage
     * @param start    start
     */
    private void insertAddress(DataPage dataPage, final int start) {
        final int freeStart = this.freeStart();
        int dataStart = start;
        // 数据开始 往后移动
        System.arraycopy(data, dataStart, data, dataStart + 8, freeStart - dataStart);
        // 地址值数量+1
        int dataAddrLen = addressLength(dataStart - 4) + 1;
        byte[] dataAddrLenBytes = ByteUtil.toBytes(dataAddrLen);
        System.arraycopy(dataAddrLenBytes, 0, data, dataStart - 4, 4);
        // 直接插入空出来的位置上   写入 数据地址值
        // 数据地址值：8字节，其中2字节表示数据文件id，2字节表示页偏移，2字节表示offset，2字节表示length
        for (byte b : dataPage.toAddress()) {
            data[dataStart++] = b;
        }
        // 更正头部 freeStart 字段
        updateFreeStart(freeStart + 8);
        this.save(); // 覆盖回写索引
    }

    public InsertAddressResult addNewAddressAndSave(final DataPage dataPage, final byte[] value, final int dataStart) {
        final Persistence persistence = PersistenceFactory.get();
        final int freeStart = this.freeStart();
        final int freeEnd = this.freeEnd();
        final int dataStartOrigin = dataStart;
        final int valCount = value.length + 2 + 4 + 8; // 2是value的长度，4索引中存储的LEN，8为数据地址8字节
        int updateBranchType = 0;         // 结果1
        LeafIndexPage changedPage = null; // 结果2
        // ---------------------- 新增不重复的索引值
        if (freeEnd - freeStart >= valCount) { // 能放下一条记录，直接插入了哈
//                new DirectInsertHandler(tablePath, column, value, dataPage, toBeUpdated).insert(leafPage, dataStart);
            // 数据开始 往后移动
            System.arraycopy(data, dataStart, data, dataStart + valCount, freeStart - dataStart);
            // 直接插入空出来的位置上
            this.addIndexValue(dataPage, value, dataStart);
            // 更正头部 freeStart 字段
            this.updateFreeStart(freeStart + valCount);
            // 覆盖回写索引
            this.save();
            if (dataStartOrigin == StorageConst.INDEX_LEAF_START) {
                updateBranchType = 1;
                changedPage = this;
            }
        } else {
            // 放不下了，需要新开一个页，此时可能需要判断插入前面还是后面
            // 遇到最小值，则新开一页并写入第一页，此时需要同步插入父索引
            // 遇到最大值则新开一个第二页写入开头，此时需要同步插入父索引
            updateBranchType = 2;
            if (dataStartOrigin == StorageConst.INDEX_LEAF_START) {
                LeafIndexPage page1 = persistence.getInsertableLeafIndex(tablePath, columnName);
//                final byte[] newBuf1 = IndexPage.newLeafBuffer();
                // 2. 写入值   最小值 直接写入第一页
                page1.addIndexValue(dataPage, value, dataStart);
                // 更新1的Next为2, prev为2的prev
                System.arraycopy(this.toAddress(), 0, page1.data, StorageConst.INDEX_LEAF_NEXT_PAGE, 4); // 1 next
                System.arraycopy(this.data, StorageConst.INDEX_LEAF_PREV_PAGE, page1.data, StorageConst.INDEX_LEAF_PREV_PAGE, 4); // 1 prev
                page1.save();
                // 更新2的prev为1
                System.arraycopy(page1.toAddress(), 0, this.data, StorageConst.INDEX_LEAF_PREV_PAGE, 4); // 2 prev
                this.save();
                changedPage = page1;
            } else { // 中间值 和 最大值, 处理结果类似，先将原数据分裂, start后的数据拷贝到第二页
                LeafIndexPage page2 = persistence.getInsertableLeafIndex(tablePath, columnName);
//                final byte[] newBuf2 = IndexPage.newLeafBuffer();
                // 拷贝1后面的数据到2
                System.arraycopy(this.data, dataStartOrigin, page2.data, StorageConst.INDEX_LEAF_START, freeStart - dataStartOrigin);
                final int freeStart1 = dataStartOrigin;
                final int freeStart2 = StorageConst.INDEX_LEAF_START + freeStart - dataStartOrigin;
                // 判断够不够写入的
                if (valCount <= freeEnd - freeStart2) { // 2够,写入2开头
                    System.arraycopy(page2.data, StorageConst.INDEX_LEAF_START, page2.data, StorageConst.INDEX_LEAF_START + valCount, freeStart - dataStartOrigin);
                    page2.addIndexValue(dataPage, value, StorageConst.INDEX_LEAF_START);
                } else if (valCount <= freeEnd - freeStart1) { // 1够,写入1末尾
                    this.addIndexValue(dataPage, value, freeStart1);
                } else { // 都不够,很尴尬的情况,需要新开一页来写入本索引,这种情况处理起来比较复杂, 简单处理 后续直接递归一下吧
                    throw new RuntimeException("分页后仍然不够存储一个索引的, 暂时放弃治疗了"); // TODO
                }
                // 更正2的prev为1，next为1的next
                System.arraycopy(this.toAddress(), 0, page2.data, StorageConst.INDEX_LEAF_PREV_PAGE, 4); // 2 prev
                System.arraycopy(this.data, StorageConst.INDEX_LEAF_NEXT_PAGE, page2.data, StorageConst.INDEX_LEAF_NEXT_PAGE, 4); // 2 next
                page2.save();
                // 更正1的next为2
                System.arraycopy(page2.toAddress(), 0, this.data, StorageConst.INDEX_LEAF_NEXT_PAGE, 4); // 1 next
                this.save();
                changedPage = page2;
            }
        }
        return new InsertAddressResult(updateBranchType, changedPage);
    }

    public static class InsertAddressResult {
        private static final InsertAddressResult EMPTY = new InsertAddressResult(0, null);
        public final int updateBranchType;
        public final LeafIndexPage changedPage;

        InsertAddressResult(int updateBranchType, LeafIndexPage changedPage) {
            this.updateBranchType = updateBranchType;
            this.changedPage = changedPage;
        }
    }
}
