package com.yuanzhy.sqldog.server.storage.disk;

import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.common.model.DataPage;
import com.yuanzhy.sqldog.server.common.model.IndexPage;
import com.yuanzhy.sqldog.server.common.model.UpdatedIndex;
import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.core.Persistence;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.storage.persistence.PersistenceFactory;
import com.yuanzhy.sqldog.server.util.ByteUtil;
import org.apache.commons.lang3.ArrayUtils;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedList;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/24
 */
public class DiskTableIndex {
    
    private final Table table;
    private final String tablePath;
    private final Persistence persistence;
    DiskTableIndex(Table table, String tablePath) {
        this.table = table;
        this.tablePath = tablePath;
        this.persistence = PersistenceFactory.get();
    }

    public boolean isConflict(String[] colNames, byte[][] values) {
        for (int i = 0; i < colNames.length; i++) {
            IndexPage rootPage = persistence.readIndex(tablePath, colNames[i]);
            Column column = table.getColumn(colNames[i]);
            IndexPage leafPage = findLeafIndex(rootPage, column, values[i], null);
            byte[] leafBuf = leafPage.getData();
            LeafResult lr = findLeafStart(column, values[i], leafBuf);
            if (lr.isNew) { // 如果联合唯一有一个是新增的就不会冲突，否则继续匹配下一字段
                return false;
            }
        }
        return true;
    }

    public void insertIndex(Column column, byte[] value, DataPage dataPage) {
        final String colName = column.getName();
        // 读取第一页 即根索引
        final IndexPage rootPage = persistence.readIndex(tablePath, colName);
        if (rootPage == null) { // 首次插入索引
            insertIndexFirst(colName, dataPage, value);
            return;
        }
        // --------------------------------------------------------------------------------
        // 查找相关节点

        // 根存储内容：最小值-索引地址值，最小值-索引地址值，最小值-索引地址值，最小值-索引地址值
        // 1. 先读取第一条，如果比第一个还小，则需要插入叶子索引的第一个位置，并更新根节点的第一条记录，即 最小值-索引地址值 --- 结束分支
        // 2. 如果比第一条大，则再循环读取后续记录并同时判断是否大于读取的值直到遇到了“小于的情况”，则退出循环 回退并取出上一记录的索引地址值，
        //    取出的地址值用于获取叶子索引页，获取时先判断一下索引level是否是0，如果不是则循环步骤, 否则进入3
        //    读取到freeStart后还没有遇到“小于的情况”，则取最后一个记录的索引地址值，找到最大的页在其最后插入索引  --- 结束分支
        // 3. 取到叶子索引页，将内部的数据和“待插入的值”重新排序写入page, 如果page没有满，则结束   --- 结束分支
        //    page满了，在当前索引页所在的文件最后新开一个页，重新排序写入这两个页并更新其PREV_PAGE NEXT_PAGE，和NEXT_PAGE中的PREV_PAGE，
        //    由于叶子加页了，树枝索引页需要插入一条记录，并重新排序，如果一页不够则新开一页步骤同上（树枝不需要PREV_NEXT_PAGE）
        //    直到写入到根节点为止，如果根页也满了，则降级为普通树枝页，同时将0号位置腾出给新的根页，此时树的层级+1  --- 结束分支
        // ----------------- begin ------------------
        // 带更新的树枝索引，按照从下到上（即最后一个为根节点）的顺序
        final LinkedList<UpdatedIndex> toBeUpdated = new LinkedList<>();
        IndexPage leafPage = findLeafIndex(rootPage, column, value, toBeUpdated);
        final LeafResult lr = findLeafStart(column, value, leafPage.getData());
        if (lr.leafPage != null) {
            leafPage = lr.leafPage;
        }
        final byte[] leafBuf = leafPage.getData();
        int freeStart = ByteUtil.toShort(leafBuf, StorageConst.FREE_START_OFFSET);
        int freeEnd = ByteUtil.toShort(leafBuf, StorageConst.FREE_END_OFFSET);
        int dataStart = lr.dataStart;
        final int dataStartOrigin = dataStart;
        final int valCount = value.length + 2 + 4 + 8; // 2是value的长度，4索引中存储的LEN，8为数据地址8字节
        if (valCount >= StorageConst.PAGE_SIZE - StorageConst.INDEX_LEAF_START) {
            // TODO
            throw new RuntimeException("单条索引大于一页的情况暂未实现：" + valCount);
        }
        int updateBranchType = 0; // 0 不更新，1. 更新值  2.插入值
        IndexPage changedPage = null;
        if (lr.isNew == false) { // 说明是重复的值
            if (freeEnd - freeStart >= 8) { // 重复的情况只要能放入一个数据地址值就可以了
                // 数据开始 往后移动
                System.arraycopy(leafBuf, dataStart, leafBuf, dataStart + 8, freeStart - dataStart);
                // 直接插入空出来的位置上   写入 数据地址值
                // 数据地址值：8字节，其中2字节表示数据文件id，2字节表示页偏移，2字节表示offset，2字节表示length
                for (byte b : dataPage.toAddress()) {
                    leafBuf[dataStart++] = b;
                }
                // 更正头部 freeStart 字段
                updateFreeStart(freeStart + 8, leafBuf);
                // 覆盖回写索引
                persistence.writeIndex(tablePath, column.getName(), leafPage);
            } else { // 8字节的地址值也放不下了，直接分裂吧
                // TODO 测一测等值的情况
                // dataStart后的数据后移
                final byte[] newBuf2 = IndexPage.newLeafBuffer();
                // 拷贝1后面的数据到2
                System.arraycopy(leafBuf, dataStartOrigin, newBuf2, StorageConst.INDEX_LEAF_START, freeStart - dataStartOrigin);
                int freeStart1 = dataStartOrigin;
                int freeStart2 = StorageConst.INDEX_LEAF_START + freeStart - dataStartOrigin;
                // 判断够不够写入的
                if (8 <= freeEnd - freeStart1) { // 1够,写入1末尾
                    for (byte b : dataPage.toAddress()) {
                        leafBuf[freeStart1++] = b;
                    }
                    updateBranchType = 2; // ----- 写入1末尾的情况 需要更新2页的父索引
                } else if (8 <= freeEnd - freeStart2) { // 2够,写入2开头
                    System.arraycopy(newBuf2, StorageConst.INDEX_LEAF_START, newBuf2, StorageConst.INDEX_LEAF_START + 8, freeStart - dataStartOrigin);
                    // 写入地址值
                    System.arraycopy(dataPage.toAddress(), 0, newBuf2, StorageConst.INDEX_LEAF_START, 8);
                    freeStart2 += 8;
                } else {
                    throw new RuntimeException("这种情况应该不存在");
                }
                // 更正1和2的头部
                updateFreeStart(freeStart1, leafBuf);
                updateFreeStart(freeStart2, newBuf2);
                // 更正2的prev为1，next为1的next
                System.arraycopy(leafPage.toAddress(), 0, newBuf2, StorageConst.INDEX_LEAF_PREV_PAGE, 4); // 2 prev
                System.arraycopy(leafBuf, StorageConst.INDEX_LEAF_NEXT_PAGE, newBuf2, StorageConst.INDEX_LEAF_NEXT_PAGE, 4); // 2 next
                IndexPage page2 = persistence.writeIndex(tablePath, colName, newBuf2);
                // 更正1的next为2
                System.arraycopy(page2.toAddress(), 0, leafBuf, StorageConst.INDEX_LEAF_NEXT_PAGE, 4); // 1 next
                persistence.writeIndex(tablePath, colName, leafPage);
                changedPage = page2;
            }
        } else {
            // ---------------------- 新增不重复的索引值
            if (freeEnd - freeStart >= valCount) { // 能放下一条记录，直接插入了哈
//                new DirectInsertHandler(tablePath, column, value, dataPage, toBeUpdated).insert(leafPage, dataStart);
                // 数据开始 往后移动
                System.arraycopy(leafBuf, dataStart, leafBuf, dataStart + valCount, freeStart - dataStart);
                // 直接插入空出来的位置上
                writeIndexValue(dataPage, value, dataStart, leafBuf);
                // 更正头部 freeStart 字段
                updateFreeStart(freeStart + valCount, leafBuf);
                // 覆盖回写索引
                persistence.writeIndex(tablePath, column.getName(), leafPage);
                if (dataStartOrigin == StorageConst.INDEX_LEAF_START) {
                    updateBranchType = 1;
                    changedPage = leafPage;
                }
            } else {
                // 放不下了，需要新开一个页，此时可能需要判断插入前面还是后面
                // 遇到最小值，则新开一页并写入第一页，此时需要同步插入父索引
                // 遇到最大值则新开一个第二页写入开头，此时需要同步插入父索引
                updateBranchType = 2;
                if (dataStartOrigin == StorageConst.INDEX_LEAF_START) {
                    final byte[] newBuf1 = IndexPage.newLeafBuffer();
                    // 2. 写入值   最小值 直接写入第一页
                    dataStart = writeIndexValue(dataPage, value, dataStart, newBuf1);
                    // 更新1的Next为2, prev为2的prev
                    System.arraycopy(leafPage.toAddress(), 0, newBuf1, StorageConst.INDEX_LEAF_NEXT_PAGE, 4); // 1 next
                    System.arraycopy(leafBuf, StorageConst.INDEX_LEAF_PREV_PAGE, newBuf1, StorageConst.INDEX_LEAF_PREV_PAGE, 4); // 1 prev
                    IndexPage page1 = persistence.writeIndex(tablePath, colName, newBuf1); // 第一页为新增的，返回地址相关信息
                    // 更新2的prev为1
                    System.arraycopy(page1.toAddress(), 0, leafPage.getData(), StorageConst.INDEX_LEAF_PREV_PAGE, 4); // 2 prev
                    persistence.writeIndex(tablePath, colName, leafPage);
                    changedPage = page1;
                } else { // 中间值 和 最大值, 处理结果类似，先将原数据分裂, start后的数据拷贝到第二页
                    final byte[] newBuf2 = IndexPage.newLeafBuffer();
                    // 拷贝1后面的数据到2
                    System.arraycopy(leafBuf, dataStartOrigin, newBuf2, StorageConst.INDEX_LEAF_START, freeStart - dataStartOrigin);
                    int freeStart1 = dataStartOrigin;
                    int freeStart2 = StorageConst.INDEX_LEAF_START + freeStart - dataStartOrigin;
                    // 判断够不够写入的
                    if (valCount <= freeEnd - freeStart2) { // 2够,写入2开头
                        System.arraycopy(newBuf2, StorageConst.INDEX_LEAF_START, newBuf2, StorageConst.INDEX_LEAF_START + valCount, freeStart - dataStartOrigin);
                        writeIndexValue(dataPage, value, StorageConst.INDEX_LEAF_START, newBuf2);
                        freeStart2 += valCount;
                    } else if (valCount <= freeEnd - freeStart1) { // 1够,写入1末尾
                        freeStart1 = writeIndexValue(dataPage, value, freeStart1, leafBuf);
                    } else { // 都不够,很尴尬的情况,需要新开一页来写入本索引,这种情况处理起来比较复杂, 简单处理 后续直接递归一下吧
                        throw new RuntimeException("分页后仍然不够存储一个索引的, 暂时放弃治疗了"); // TODO
                    }
                    // 更正1和2的头部
                    updateFreeStart(freeStart1, leafBuf);
                    updateFreeStart(freeStart2, newBuf2);
                    // 更正2的prev为1，next为1的next
                    System.arraycopy(leafPage.toAddress(), 0, newBuf2, StorageConst.INDEX_LEAF_PREV_PAGE, 4); // 2 prev
                    System.arraycopy(leafBuf, StorageConst.INDEX_LEAF_NEXT_PAGE, newBuf2, StorageConst.INDEX_LEAF_NEXT_PAGE, 4); // 2 next
                    IndexPage page2 = persistence.writeIndex(tablePath, colName, newBuf2);
                    // 更正1的next为2
                    System.arraycopy(page2.toAddress(), 0, leafBuf, StorageConst.INDEX_LEAF_NEXT_PAGE, 4); // 1 next
                    persistence.writeIndex(tablePath, colName, leafPage);
                    changedPage = page2;
                }
            }
        }
        if (updateBranchType == 0) {
            return;
        }
        for (int i = 0; i < toBeUpdated.size(); i++) {
            final IndexPage lowerIndex = i == 0 ? changedPage : toBeUpdated.get(i - 1).getIndexPage();
            final UpdatedIndex updatedIndex = toBeUpdated.get(i);
            final byte[] updateBuf = updatedIndex.getIndexPage().getData();
            final byte level = updateBuf[StorageConst.INDEX_LEVEL_START];
            dataStart = updatedIndex.getDataStart(); // 待插入的位置，【值-索引地址值】
            freeStart = ByteUtil.toShort(updateBuf, StorageConst.FREE_START_OFFSET);
            freeEnd = ByteUtil.toShort(updateBuf, StorageConst.FREE_END_OFFSET);
            final int branchValCount = value.length + 2 + 4; // 2数据长度，4索引地址长度
            if (updateBranchType == 1) { // 替换最小值情况
                int minValLen = ByteUtil.toShort(updateBuf, dataStart);
                int minValCount = 2 + minValLen + 4;
                if (freeEnd - freeStart + minValCount >= branchValCount) { // 不需要分页
                    if (branchValCount == minValCount) { // 正好, 直接替换写入, 头都不用更了
                        writeIndexValue(lowerIndex, value, StorageConst.INDEX_BRANCH_START, updateBuf);
                        persistence.writeIndex(tablePath, colName, updatedIndex.getIndexPage());
                    } else { // 需要左移或右移
                        System.arraycopy(updateBuf, StorageConst.INDEX_BRANCH_START + minValCount,
                                updateBuf, StorageConst.INDEX_BRANCH_START + branchValCount,
                                freeEnd - StorageConst.INDEX_BRANCH_START - minValCount);
                        writeIndexValue(lowerIndex, value, StorageConst.INDEX_BRANCH_START, updateBuf);
                        updateFreeStart(freeStart + branchValCount - minValCount, updateBuf);
                        persistence.writeIndex(tablePath, colName, updatedIndex.getIndexPage());
                    }
                } else { // 需要分页, 替换你都不够, 太虽了
                    // 向左插入一页新的
                    byte[] newBuf1 = IndexPage.newBuffer(level);
                    int freeStart1 = writeIndexValue(lowerIndex, value, StorageConst.INDEX_BRANCH_START, newBuf1);
                    updateFreeStart(freeStart1, newBuf1);
                    IndexPage page1 = persistence.writeIndex(tablePath, colName, newBuf1);
                    if (i == toBeUpdated.size() - 1) {
                        // 当前是根节点了，需要升级 =======================================
                        IndexPage page2 = persistence.writeIndex(tablePath, colName, updateBuf); // 将原根写入其他位置, 把0号位置让给新根
                        byte[] rootBuf = rootPage.getData();
                        rootBuf[StorageConst.INDEX_LEVEL_START] = (byte)(level+1); // 写入level
                        int rootStart = writeIndexValue(page1, value, StorageConst.INDEX_BRANCH_START, rootBuf); // 写入第一页的地址
                        // 写入第二页的地址
                        int len2 = ByteUtil.toShort(updateBuf, StorageConst.INDEX_BRANCH_START);
                        byte[] val2 = ArrayUtils.subarray(updateBuf, StorageConst.INDEX_BRANCH_START + 2, StorageConst.INDEX_BRANCH_START + 2 + len2);
                        rootStart = writeIndexValue(page2, val2, rootStart, rootBuf);
                        updateFreeStart(rootStart, rootBuf); // 更新新根的freeStart头信息
                        // 保存新根
                        persistence.writeIndex(tablePath, colName, rootPage);
                    } else {
                        // 父索引需要新增了, 改为2. 前置索引改为新增的索引
                        updateBranchType = 2;
                        toBeUpdated.set(i, new UpdatedIndex(page1, StorageConst.INDEX_BRANCH_START, value));
                    }
                }
            } else {
                // 新增的情况
                if (freeEnd - freeStart >= branchValCount) { // 够了, start后的数据右移
                    System.arraycopy(updateBuf, dataStart, updateBuf, dataStart + branchValCount, freeEnd - dataStart);
                    writeIndexValue(lowerIndex, value, dataStart, updateBuf);
                    updateFreeStart(freeStart + branchValCount, updateBuf);
                    persistence.writeIndex(tablePath, colName, updatedIndex.getIndexPage());
                    // --------------------------------------
                    // 由于够了, 没有新增页, 父索引不需要更新了
                    break;
                    // --------------------------------------
                } else { // 不够了, 新开页
                    IndexPage page1 = null, page2 = null;
                    // 分裂, 最小值特殊处理. 插入左页, 2页没有变化所以不用更了
                    if (dataStart == StorageConst.INDEX_BRANCH_START) {
                        final byte[] newBuf1 = IndexPage.newBuffer(level);
                        // 2. 写入值   最小值 直接写入第一页
                        writeIndexValue(lowerIndex, value, dataStart, newBuf1);
                        page1 = persistence.writeIndex(tablePath, colName, newBuf1); // 第一页为新增的，返回地址相关信息
                        toBeUpdated.set(i, new UpdatedIndex(page1, dataStart, value));
                    } else { // 最大值和中间值 逻辑类似. 可能会有拷贝到第二页的数据为空的情况, 即参数5长度为0
                        final byte[] newBuf2 = IndexPage.newBuffer(level);
                        // 拷贝1后面的数据到2
                        System.arraycopy(leafBuf, dataStart, newBuf2, StorageConst.INDEX_BRANCH_START, freeStart - dataStart);
                        int freeStart1 = dataStart;
                        int freeStart2 = StorageConst.INDEX_BRANCH_START + freeStart - dataStart;
                        // 判断够不够写入的
                        if (branchValCount <= freeEnd - freeStart2) { // 2够,写入2开头
                            System.arraycopy(newBuf2, StorageConst.INDEX_BRANCH_START, newBuf2, StorageConst.INDEX_BRANCH_START + branchValCount, freeStart - dataStart);
                            writeIndexValue(lowerIndex, value, StorageConst.INDEX_BRANCH_START, newBuf2);
                            freeStart2 += branchValCount;
                        } else if (branchValCount <= freeEnd - freeStart1) { // 1够,写入1末尾
                            freeStart1 = writeIndexValue(lowerIndex, value, freeStart1, updateBuf);
                        } else { // 都不够, 这种情况不可能, 因为会限制大于1000长度的字段禁止建立索引
                            throw new RuntimeException("分页后仍然不够存储一个索引的, 暂时放弃治疗了");
                        }
                        // 更正1和2的头部
                        updateFreeStart(freeStart1, updateBuf);
                        updateFreeStart(freeStart2, newBuf2);
                        // 写入新页2 和 原页
                        page2 = persistence.writeIndex(tablePath, colName, newBuf2);
                        persistence.writeIndex(tablePath, colName, updatedIndex.getIndexPage());
                        toBeUpdated.set(i, new UpdatedIndex(page2, freeStart2, value));
                    }
                    if (i == toBeUpdated.size() - 1) {
                        // 当前是根节点了，需要升级 =======================================
                        if (page1 == null) { // 谁为空谁就是原根
                            page1 = persistence.writeIndex(tablePath, colName, updateBuf); // 将原根写入其他位置, 把0号位置让给新根
                        } else {
                            page2 = persistence.writeIndex(tablePath, colName, updateBuf); // 将原根写入其他位置, 把0号位置让给新根
                        }
                        byte[] rootBuf = rootPage.getData();
                        rootBuf[StorageConst.INDEX_LEVEL_START] = (byte)(level+1); // 写入level
                        int len1 = ByteUtil.toShort(page1.getData(), StorageConst.INDEX_BRANCH_START);
                        byte[] val1 = ArrayUtils.subarray(page1.getData(), StorageConst.INDEX_BRANCH_START + 2, StorageConst.INDEX_BRANCH_START + 2 + len1);
                        int rootStart = writeIndexValue(page1, val1, StorageConst.INDEX_BRANCH_START, rootBuf); // 写入第一页的地址
                        // 写入第二页的地址
                        int len2 = ByteUtil.toShort(page2.getData(), StorageConst.INDEX_BRANCH_START);
                        byte[] val2 = ArrayUtils.subarray(page2.getData(), StorageConst.INDEX_BRANCH_START + 2, StorageConst.INDEX_BRANCH_START + 2 + len2);
                        rootStart = writeIndexValue(page2, val2, rootStart, rootBuf);
                        updateFreeStart(rootStart, rootBuf); // 更新新根的freeStart头信息
                        // 保存新根
                        persistence.writeIndex(tablePath, colName, rootPage);
                    }
                }
            }
        }
    }

    private void insertIndexFirst(String colName, DataPage dataPage, byte[] value) {
        // 先写入叶子节点
        byte[] leafBuf = IndexPage.newLeafBuffer();
        //  - PREV_PAGE and NEXT_PAGE is 0 表示无
        // 写入索引数据：[值-LENGTH-数据地址值]
        //  索引地址值：4字节，其中2字节表示索引文件id，2字节表示页偏移
        // 上一页下一页地址为0，需要空出16字节
        int freeStart = StorageConst.INDEX_LEAF_START; // 叶子节点起始
        freeStart = writeIndexValue(dataPage, value, freeStart, leafBuf);
        updateFreeStart(freeStart, leafBuf);
        IndexPage leafPage = new IndexPage(StorageConst.INDEX_DEF_FILE_ID, 1, leafBuf);
        // --------------------------------------------------------------------------------
        // 再写入根索引
        byte[] rootBuf = IndexPage.newBuffer(1);
        // 非叶子节点不含 PREV_PAGE and NEXT_PAGE
        // 写入索引数据：[值-索引地址值]
        freeStart = StorageConst.INDEX_BRANCH_START;
        freeStart = writeIndexValue(leafPage, value, freeStart, rootBuf);
        updateFreeStart(freeStart, rootBuf);
        persistence.writeIndex(tablePath, colName, new IndexPage(StorageConst.INDEX_DEF_FILE_ID, rootBuf));
        persistence.writeIndex(tablePath, colName, leafPage);
    }

    private void updateFreeStart(int freeStart, byte[] newBuf) {
        byte[] startBytes = ByteUtil.toBytes((short)freeStart);
        newBuf[StorageConst.FREE_START_OFFSET] = startBytes[0];
        newBuf[StorageConst.FREE_START_OFFSET+1] = startBytes[1];
    }

    private int writeIndexValue(DataPage dataPage, byte[] value, int dataStart, byte[] newBuf) {
        //   写入长度和值
        byte[] valuelen = ByteUtil.toBytes((short) value.length);
        System.arraycopy(valuelen, 0, newBuf, dataStart, 2);
        dataStart += 2;
        for (byte v : value) {
            newBuf[dataStart++] = v;
        }
        //     写入 LEN
        byte[] addrLen = ByteUtil.toBytes(1); // 首次插入值，LEN为1，用4字节表示
        for (byte v : addrLen) {
            newBuf[dataStart++] = v;
        }
        //     写入 数据地址值
        // 数据地址值：8字节，其中2字节表示数据文件id，2字节表示页偏移，2字节表示offset，2字节表示length
        for (byte b : dataPage.toAddress()) {
            newBuf[dataStart++] = b;
        }
        return dataStart;
    }

    private int writeIndexValue(IndexPage indexPage, byte[] value, int dataStart, byte[] newBuf) {
        byte[] valuelen = ByteUtil.toBytes((short)value.length);
        System.arraycopy(valuelen, 0, newBuf, dataStart, 2);
        dataStart += 2;
        for (byte v : value) {
            newBuf[dataStart++] = v;
        }
        // 写入 索引地址值
        // 索引地址值：4字节，其中2字节表示索引文件id，2字节表示页偏移
        for (byte b : indexPage.toAddress()) {
            newBuf[dataStart++] = b;
        }
        return dataStart;
    }

    private LeafResult findLeafStart(Column column, byte[] value, byte[] leafBuf) {
        int dataStart = StorageConst.INDEX_LEAF_START;
        int freeStart = ByteUtil.toShort(leafBuf, StorageConst.FREE_START_OFFSET);
        byte[] existsVal;
        int valLength = ByteUtil.toShort(leafBuf, dataStart);
        dataStart += 2;
        existsVal = ArrayUtils.subarray(leafBuf, dataStart, dataStart += valLength);
        int compared = compare(column, value, existsVal);
        if (compared == 0) {
            // 和最小值相等，叶子长度+1
            int dataAddrLen = ByteUtil.toInt(leafBuf, dataStart);
            dataStart += 4 + 8 * dataAddrLen; // 跳过地址长度，定位到数据地址的开头
            IndexPage nextPage = null;
            while (dataStart > StorageConst.PAGE_SIZE) {
                nextPage = IndexPage.fromAddress(leafBuf, StorageConst.INDEX_LEAF_NEXT_PAGE);
                nextPage = persistence.readIndex(tablePath, column.getName(), nextPage.getFileId(), nextPage.getOffset());
                dataStart -= StorageConst.PAGE_SIZE;
            }
            return new LeafResult(false, dataStart, nextPage);
        } else if (compared < 0) {
            // 比最小值还小，直接插入最左边.
            dataStart -= 2 + valLength; // 指针移动到上一条记录的最后
//                    if (dataStart == StorageConst.INDEX_LEAF_START) { // 前面已经没有数据了，直接取第一条数据对应的索引地址
//                        dataStart += 2 + valLength;
//                    } else {
//                        dataStart -= 4; // 获取前一个值对应的索引地址
//                    }
            return new LeafResult(true, dataStart);
        } else { // > 0
            boolean found = true;
            IndexPage nextPage = null;
            do {
                int dataAddrLen = ByteUtil.toInt(leafBuf, dataStart);
                dataStart += 4 + 8 * dataAddrLen;  // 跳过地址值数量
                if (dataStart > freeStart) {
                    // TODO 不算数据异常，可能跳页了， 需要从下一页继续, 这块需要测测
                    while (dataStart > StorageConst.PAGE_SIZE) {
                        nextPage = IndexPage.fromAddress(leafBuf, StorageConst.INDEX_LEAF_NEXT_PAGE);
                        nextPage = persistence.readIndex(tablePath, column.getName(), nextPage.getFileId(), nextPage.getOffset());
                        dataStart -= StorageConst.PAGE_SIZE;
                    }
                    if (nextPage == null) {
                        throw new RuntimeException("数据异常");
                    }
                    leafBuf = nextPage.getData();
                    freeStart = ByteUtil.toShort(leafBuf, StorageConst.FREE_START_OFFSET);
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
                return new LeafResult(false, dataStart, nextPage);
            }
            return new LeafResult(true, dataStart, nextPage);
        }
//        return dataStart;
    }

    // 寻找叶子节点，并收集需要更新的树枝节点信息
    private IndexPage findLeafIndex(IndexPage pPage, Column column, byte[] value, LinkedList<UpdatedIndex> toBeUpdated) {
        byte[] buf = pPage.getData();
        int freeStart = buf[StorageConst.FREE_START_OFFSET];
        int dataStart = StorageConst.INDEX_BRANCH_START;
        byte[] existsVal;
        int valLength = ByteUtil.toShort(buf, dataStart);
        dataStart += 2;
        existsVal = ArrayUtils.subarray(buf, dataStart, dataStart += valLength);
        short fileId, pageOffset;
        int compared = compare(column, value, existsVal);
        if (compared == 0) {
            // 取相等的地址值
            if (toBeUpdated != null)
                toBeUpdated.addFirst(new UpdatedIndex(pPage, dataStart+4, value)); // 跳过地址值
        } else if (compared < 0) {
            // 比最小值还小，直接插入最左边.
            dataStart -= 2 + valLength; // 指针移动到上一条记录的最后
            if (toBeUpdated != null)
                toBeUpdated.addFirst(new UpdatedIndex(pPage, dataStart, value));
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
            if (found) {
                dataStart -= 2 + valLength; // 找到比插入的值大的了，这时寻找前一个值的地址
            } // else { // 遍历到最后还没有，说明新插入的值是最大的，直接找最后一页的地址}
            if (toBeUpdated != null)
                toBeUpdated.addFirst(new UpdatedIndex(pPage, dataStart, value));
            dataStart -= 4;
        }
        // 获取 索引地址值
        fileId = ByteUtil.toShort(buf, dataStart);
        dataStart += 2;
        pageOffset = ByteUtil.toShort(buf, dataStart);
        // 根据父索引获取直接子索引
        final IndexPage indexPage = persistence.readIndex(tablePath, column.getName(), fileId, pageOffset);
        byte[] indexBuf = indexPage.getData();
        byte level = indexBuf[StorageConst.INDEX_LEVEL_START];
        if (level > 0) { // 树枝
            return findLeafIndex(indexPage, column, value, toBeUpdated);
        } else {
            return indexPage;
        }
    }

    private int compare(Column column, byte[] v1, byte[] v2) {
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

    private class LeafResult {
        boolean isNew; // 是否新增，如果有相同的索引值，则为false
        int dataStart;

        IndexPage leafPage;
        LeafResult(boolean isNew, int dataStart) {
            this(isNew, dataStart, null);
        }

        LeafResult(boolean isNew, int dataStart, IndexPage leafPage) {
            this.isNew = isNew;
            this.dataStart = dataStart;
            this.leafPage = leafPage;
        }
    }
}
