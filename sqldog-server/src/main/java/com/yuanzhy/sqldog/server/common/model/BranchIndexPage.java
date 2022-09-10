package com.yuanzhy.sqldog.server.common.model;

import java.util.LinkedList;

import com.yuanzhy.sqldog.core.util.ArrayUtils;
import com.yuanzhy.sqldog.core.util.ByteUtil;
import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.core.Persistence;
import com.yuanzhy.sqldog.server.storage.persistence.PersistenceFactory;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/8/27
 */
public class BranchIndexPage extends IndexPage {

    public BranchIndexPage(String tablePath, String columnName, short fileId) {
        this(tablePath, columnName, fileId, 0);
    }

    public BranchIndexPage(String tablePath, String columnName, short fileId, int offset) {
        this(tablePath, columnName, fileId, offset, newBuffer());
    }

    BranchIndexPage(String tablePath, String columnName, short fileId, int offset, byte[] data) {
        super(tablePath, columnName, fileId, offset, data);
    }

    @Override
    public BranchIndexPage copyTo(short fileId) {
        return new BranchIndexPage(tablePath, columnName, fileId, 0, data);
    }

    @Override
    public boolean isEmpty() {
        return freeStart() == StorageConst.INDEX_BRANCH_START;
    }

    @Override
    public void clear() {
        updateFreeStart(StorageConst.INDEX_BRANCH_START);
    }

    @Override
    public byte[] minValue() { // 获取索引页的最小值
        return this.value(StorageConst.INDEX_BRANCH_START);
    }

    public int addIndexValue(IndexPage indexPage, byte[] value, int dataStart) {
        dataStart = replaceIndexValue(indexPage, value, dataStart);
        updateFreeStart(dataStart);
        return dataStart;
    }

    public int addIndexValue(IndexPage indexPage, byte[] value) {
        return addIndexValue(indexPage, value, StorageConst.INDEX_BRANCH_START);
    }

    public void deleteIndexValue(byte[] value, int dataStart) {
        int nextValueStart = 2 + value.length + 4;
        System.arraycopy(data, nextValueStart, data, dataStart + nextValueStart, freeStart() - nextValueStart);
    }

    public int replaceIndexValue(IndexPage indexPage, byte[] value, int dataStart) {
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
        for (byte b : indexPage.toAddress()) {
            data[dataStart++] = b;
        }
        return dataStart;
    }

    /**
     * 替换索引数据, 根据旧数据长度左移或右移数据
     * @param indexPage 下级索引页
     * @param value      新值
     * @param oldValue   旧值
     * @param dataStart  起始位置
     */
    public void replaceIndexValue(IndexPage indexPage, byte[] value, byte[] oldValue, int dataStart) {
        final int freeEnd = this.freeEnd();
        final int minValCount = 2 + oldValue.length + 4;
        final int branchValCount = value.length + 2 + 4; // 2数据长度，4索引地址长度
        System.arraycopy(this.data, dataStart + minValCount, this.data, dataStart + branchValCount, freeEnd - dataStart - minValCount);
        this.addIndexValue(indexPage, value, dataStart);
    }

    public void insertIndexValue(IndexPage indexPage, byte[] value, int dataStart) {
        final int freeStart = this.freeStart();
        final int branchValCount = value.length + 2 + 4; // 2数据长度，4索引地址长度
        if (freeStart != dataStart) {
            System.arraycopy(this.data, dataStart, this.data, dataStart + branchValCount, freeStart - dataStart);
        }
        this.addIndexValue(indexPage, value, dataStart);
    }

    /**
     * 插入索引，生成新页
     * @param indexPage 索引页
     * @param value     值
     * @param dataStart 起始位置
     * @return 新页
     */
    public BranchIndexPage insertIndexValueNewly(final IndexPage indexPage, final byte[] value, final int dataStart) {
        final Persistence persistence = PersistenceFactory.get();
        final int branchValCount = value.length + 2 + 4; // 2数据长度，4索引地址长度
        final int freeStart = this.freeStart();
        final int freeEnd = this.freeEnd();
        final BranchIndexPage page2 = (BranchIndexPage) persistence.newIndex(tablePath, columnName, this.level());
        // 拷贝1后面的数据到2
        System.arraycopy(this.data, dataStart, page2.data, StorageConst.INDEX_BRANCH_START, freeStart - dataStart);
        final int freeStart1 = dataStart;
        int freeStart2 = StorageConst.INDEX_BRANCH_START + freeStart - dataStart;
        // 判断够不够写入的
        if (branchValCount <= freeEnd - freeStart2) { // 2够,写入2开头
            System.arraycopy(page2.data, StorageConst.INDEX_BRANCH_START, page2.data, StorageConst.INDEX_BRANCH_START + branchValCount, freeStart - dataStart);
//            writeIndexValue(lowerIndex, value, StorageConst.INDEX_BRANCH_START, page2.data);
            page2.addIndexValue(indexPage, value);
            freeStart2 += branchValCount;
        } else if (branchValCount <= freeEnd - freeStart1) { // 1够,写入1末尾
//            freeStart1 = writeIndexValue(lowerIndex, value, freeStart1, updateBuf);
            this.addIndexValue(indexPage, value, freeStart1);
        } else { // 都不够, 这种情况不可能, 因为会限制大于1000长度的字段禁止建立索引
            throw new RuntimeException("分页后仍然不够存储一个索引的, 暂时放弃治疗了");
        }
        // 更正1和2的头部
//        this.updateFreeStart(freeStart1);
        page2.updateFreeStart(freeStart2);
        // 写入新页2 和 原页
        page2.save();
        this.save();
        return page2;
    }

    /**
     * 寻找叶子节点
     * @param column column
     * @param value  value
     * @return
     */
    public LeafIndexPage findLeafIndex(final Column column, final byte[] value) {
        return this.findLeafIndex(column, value, null);
    }

    /**
     * 寻找叶子节点，并收集需要更新的树枝节点信息
     * @param column  column
     * @param value   value
     * @param toBeUpdated toBeUpdated
     * @return
     */
    public LeafIndexPage findLeafIndex(final Column column, final byte[] value, LinkedList<UpdatedIndex> toBeUpdated) {
        return this.findLeafIndex(this, column, value, toBeUpdated);
    }

    private LeafIndexPage findLeafIndex(BranchIndexPage pPage, Column column, byte[] value, LinkedList<UpdatedIndex> toBeUpdated) {
        if (pPage.isEmpty()) {
            return null;
        }
        final byte[] buf = pPage.data;
        final int freeStart = pPage.freeStart();
        int dataStart = StorageConst.INDEX_BRANCH_START;
        byte[] existsVal = pPage.value(dataStart);
        int valLength = existsVal.length;
        dataStart += 2 + valLength; // 指针移动到值后面，索引地址值前面
        short fileId, pageOffset;
        int compared = compare(column, value, existsVal);
        if (compared == 0) {
            // 取相等的地址值
            if (toBeUpdated != null)
                toBeUpdated.addFirst(new UpdatedIndex(pPage, dataStart - valLength - 2)); // 树枝节点等值情况取值的开头
        } else if (compared < 0) {
            // 比最小值还小，直接插入最左边.
            dataStart -= 2 + valLength; // 指针移动到上一条记录的最后
            if (toBeUpdated != null)
                toBeUpdated.addFirst(new UpdatedIndex(pPage, dataStart));
            if (dataStart == StorageConst.INDEX_BRANCH_START) { // 前面已经没有数据了，直接取第一条数据对应的索引地址
                dataStart += 2 + valLength;
            } else {
                dataStart -= 4; // 获取前一个值对应的索引地址
            }
        } else { // > 0
            boolean found = true;
            // 此处无需处理等于的情况，找到了小于的值，直接取它前面的值即可，此值可以等于value, 也可以小于value.处理逻辑类似
            while (compare(column, value, existsVal) > 0) {
                dataStart += 4; // 跳过索引地址值长度
                if (dataStart > freeStart) {
                    throw new RuntimeException("数据异常");
                }
                if (dataStart == freeStart) {
                    found = false;
                    break;
                }
                valLength = ByteUtil.toShort(buf, dataStart);
                dataStart += 2;
                existsVal = ArrayUtils.subarray(buf, dataStart, dataStart += valLength);
            }
            // 获取前一个值对应的索引地址
            boolean eq = compare(column, value, existsVal) == 0;
            if (found) { // 等值的情况，树枝节点定位到值的开头，不等值的情况定位前一个地址的末尾 ==========================
                dataStart -= 2 + valLength; // 找到比插入的值大的了，这时寻找前一个值的地址 末尾
            } // else { // 遍历到最后还没有，说明新插入的值是最大的，直接找最后一页的地址}
            if (toBeUpdated != null) {
                toBeUpdated.addFirst(new UpdatedIndex(pPage, dataStart));
            }
            if (eq) {
                dataStart += 2 + valLength; // 等值，跳到等值的索引地址值位置
            } else {
                dataStart -= 4; // 不等，跳到前一个值的索引地址值位置
            }
        }
        // 获取 索引地址值
        fileId = ByteUtil.toShort(buf, dataStart);
        dataStart += 2;
        pageOffset = ByteUtil.toShort(buf, dataStart);
        // 根据父索引获取直接子索引
        final IndexPage indexPage = PersistenceFactory.get().readIndex(tablePath, column.getName(), fileId, pageOffset);
        if (indexPage.isBranch()) { // 树枝
            return findLeafIndex((BranchIndexPage) indexPage, column, value, toBeUpdated);
        } else {
            return (LeafIndexPage) indexPage;
        }
    }
}
