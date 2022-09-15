package com.yuanzhy.sqldog.server.storage.disk;

import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.common.UpdateBranchType;
import com.yuanzhy.sqldog.server.common.model.BranchIndexPage;
import com.yuanzhy.sqldog.server.common.model.DataPage;
import com.yuanzhy.sqldog.server.common.model.IndexPage;
import com.yuanzhy.sqldog.server.common.model.LeafIndexPage;
import com.yuanzhy.sqldog.server.common.model.Location;
import com.yuanzhy.sqldog.server.common.model.UpdatedIndex;
import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.core.Constraint;
import com.yuanzhy.sqldog.server.core.Persistence;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.core.constant.ConstraintType;
import com.yuanzhy.sqldog.server.storage.persistence.PersistenceFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/24
 */
public class DiskTableIndex {
    
    private final Table table;

    private final DiskTableData tableData;
    private final String tablePath;
    private final Persistence persistence;
    DiskTableIndex(Table table, String tablePath, DiskTableData tableData) {
        this.table = table;
        this.tablePath = tablePath;
        this.tableData = tableData;
        this.persistence = PersistenceFactory.get();
    }

    public boolean isConflict(String[] colNames, byte[][] values) {
        boolean isUnited = colNames.length > 1;
        List<IndexPage.LeafResult> list = isUnited ? new ArrayList<>() : null;
        for (int i = 0; i < colNames.length; i++) {
            BranchIndexPage rootPage = (BranchIndexPage) persistence.readIndex(tablePath, colNames[i]);
            if (rootPage == null) {
                return false;
            }
            Column column = table.getColumn(colNames[i]);
            LeafIndexPage leafPage = rootPage.findLeafIndex(column, values[i]);
            if (leafPage == null) {
                return false;
            }
//            byte[] leafBuf = leafPage.getData();
            IndexPage.LeafResult lr = leafPage.findLeafStart(column, values[i]);
            if (lr.isNew) { // 如果联合唯一有一个是新增的就不会冲突，否则继续匹配下一字段
                return false;
            } else if (isUnited) {
                list.add(lr);
            }
        }
        // 如果是联合唯一，查看多个键对应的地址值有没有重复的
        if (isUnited) {
            List<Object> mAddrs = new ArrayList<>();
            for (IndexPage.LeafResult lr : list) {
                final int dataAddrLen = lr.addressLength;
                int ds = lr.dataStart;
                for (int i = 0; i < dataAddrLen; i++) {
                    final byte[] addr = lr.leafPage.address(ds);
                    ds += 8;
                    for (Object mAddr : mAddrs) {
                        byte[] existsAddr = (byte[]) mAddr;
                        if (Arrays.equals(existsAddr, addr)) {
                            return true;
                        }
                    }
                    mAddrs.add(addr);
                }
            }
            return false;
        }
        return true;
    }

    public void insertIndex(Column column, byte[] value, DataPage dataPage) {
        final String colName = column.getName();
        // 读取第一页 即根索引
        final BranchIndexPage rootPage = (BranchIndexPage) persistence.readIndex(tablePath, colName);
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
        LeafIndexPage leafPage = rootPage.findLeafIndex(column, value, toBeUpdated);
        final IndexPage.LeafResult lr = leafPage.findLeafStart(column, value);
        if (lr.leafPage != null) {
            leafPage = lr.leafPage;
        }
        final int valCount = value.length + 2 + 4 + 8; // 2是value的长度，4索引中存储的LEN，8为数据地址8字节
        if (valCount >= StorageConst.PAGE_SIZE - StorageConst.INDEX_LEAF_START) {
            // TODO
            throw new RuntimeException("单条索引大于一页的情况暂未实现：" + valCount);
        }
        LeafIndexPage.InsertAddressResult iar = lr.isNew == false ? // 说明是重复的值
                leafPage.addExistingAddressAndSave(dataPage, lr.dataStart) :
                leafPage.addNewAddressAndSave(dataPage, value, lr.dataStart);
        // 0 不更新，1. 更新值  2.插入值  3.删除
        UpdateBranchType updateBranchType = iar.updateBranchType;
        LeafIndexPage changedPage = iar.changedPage;
        updateBranchIndex(colName, value, updateBranchType, toBeUpdated, changedPage);
    }

    private void updateBranchIndex(String colName, byte[] value, UpdateBranchType updateBranchType, LinkedList<UpdatedIndex> toBeUpdated, LeafIndexPage leafPage) {
        if (updateBranchType == UpdateBranchType.NOOP) {
            return;
        }
        for (int i = 0; i < toBeUpdated.size(); i++) {
            final IndexPage lowerIndex = i == 0 ? leafPage : toBeUpdated.get(i - 1).getIndexPage();
            final UpdatedIndex updatedIndex = toBeUpdated.get(i);
            final BranchIndexPage updatedPage = updatedIndex.getIndexPage();
//            final byte[] value = updatedIndex.getValue();
//            final byte[] updateBuf = updatedIndex.getIndexPage().getData();
            final int level = updatedPage.level();
            final int dataStart = updatedIndex.getDataStart(); // 待插入的位置，【值-索引地址值】
            final int freeStart = updatedPage.freeStart();
            final int freeEnd = updatedPage.freeEnd();
            final int branchValCount = value.length + 2 + 4; // 2数据长度，4索引地址长度
            if (updateBranchType == UpdateBranchType.UPDATE) { // 替换最小值情况
                final byte[] minVal = updatedPage.value(dataStart);
                final int minValLen = minVal.length;
                final int minValCount = 2 + minValLen + 4;
                if (lowerIndex.isEmpty()) { // 下级索引已经空了，删除本索引对应的最小值地址
                    updatedPage.deleteIndexValue(value, StorageConst.INDEX_BRANCH_START);
                    updatedPage.save();
                } else if (freeEnd - freeStart + minValCount >= branchValCount) { // 不需要分页
                    if (branchValCount == minValCount) { // 正好, 直接替换写入, 头都不用更了
                        updatedPage.replaceIndexValue(lowerIndex, value, StorageConst.INDEX_BRANCH_START);
                        updatedPage.save();
                    } else { // 需要左移或右移
//                        System.arraycopy(updateBuf, StorageConst.INDEX_BRANCH_START + minValCount,
//                                updateBuf, StorageConst.INDEX_BRANCH_START + branchValCount,
//                                freeEnd - StorageConst.INDEX_BRANCH_START - minValCount);
//                        writeIndexValue(lowerIndex, value, StorageConst.INDEX_BRANCH_START, updateBuf);
//                        updateFreeStart(freeStart + branchValCount - minValCount, updateBuf);
//                        persistence.writeIndex(tablePath, colName, updatedIndex.getIndexPage());
                        updatedPage.replaceIndexValue(lowerIndex, value, minVal, StorageConst.INDEX_BRANCH_START);
                        updatedPage.save();
                    }
                } else { // 需要分页, 替换你都不够, 太虽了
                    // 向左插入一页新的
                    BranchIndexPage page1 = (BranchIndexPage) persistence.newIndex(tablePath, colName, level);
//                    byte[] newBuf1 = IndexPage.newBuffer(level);
//                    int freeStart1 = writeIndexValue(lowerIndex, value, StorageConst.INDEX_BRANCH_START, newBuf1);
//                    updateFreeStart(freeStart1, newBuf1);
//                    IndexPage page1 = persistence.writeIndex(tablePath, colName, newBuf1);
                    page1.addIndexValue(lowerIndex, value);
                    page1.save();
                    if (i == toBeUpdated.size() - 1) {
                        // 当前是根节点了，需要升级 =======================================
                        BranchIndexPage rootPage = updatedIndex.getIndexPage();
                        // 将原根写入其他位置, 把0号位置让给新根
//                        IndexPage page2 = persistence.writeIndex(tablePath, colName, updateBuf);
                        BranchIndexPage page2 = (BranchIndexPage) persistence.newIndex(tablePath, colName, updatedPage.level());
                        page2 = updatedPage.copyTo(page2.getFileId());
//                        page2.save();
//                        byte[] rootBuf = rootPage.getData();
//                        int rootStart = writeIndexValue(page1, value, StorageConst.INDEX_BRANCH_START, rootBuf); // 写入第一页的地址
                        rootPage.level(level + 1);
                        final int rootStart = rootPage.replaceIndexValue(page1, value, StorageConst.INDEX_BRANCH_START); // 写入第一页的地址
                        // 写入第二页的地址
//                        int len2 = ByteUtil.toShort(updateBuf, StorageConst.INDEX_BRANCH_START);
//                        byte[] val2 = ArrayUtils.subarray(updateBuf, StorageConst.INDEX_BRANCH_START + 2, StorageConst.INDEX_BRANCH_START + 2 + len2);
//                        rootStart = writeIndexValue(page2, val2, rootStart, rootBuf);
//                        updateFreeStart(rootStart, rootBuf); // 更新新根的freeStart头信息
                        final byte[] val2 = page2.value(StorageConst.INDEX_BRANCH_START);
                        rootPage.addIndexValue(page2, val2, rootStart);
                        // 保存新根
                        rootPage.save();
                    } else {
                        // 父索引需要新增了, 改为2. 前置索引改为新增的索引
                        updateBranchType = UpdateBranchType.INSERT;
                        toBeUpdated.set(i, new UpdatedIndex(page1, StorageConst.INDEX_BRANCH_START));
                    }
                }
            } else {
                // 新增的情况
                if (freeEnd - freeStart >= branchValCount) { // 够了, start后的数据右移
//                    if (freeStart != dataStart) {
//                        System.arraycopy(updateBuf, dataStart, updateBuf, dataStart + branchValCount, freeStart - dataStart);
//                    }
//                    writeIndexValue(lowerIndex, value, dataStart, updateBuf);
//                    updateFreeStart(freeStart + branchValCount, updateBuf);
//                    persistence.writeIndex(tablePath, colName, updatedIndex.getIndexPage());
                    updatedPage.insertIndexValue(lowerIndex, value, dataStart);
                    updatedPage.save();
                    // --------------------------------------
                    // 由于够了, 没有新增页, 父索引不需要更新了
                    break;
                    // --------------------------------------
                } else { // 不够了, 新开页
                    BranchIndexPage page1 = null, page2 = null;
                    // 分裂, 最小值特殊处理. 插入左页, 2页没有变化所以不用更了
                    if (dataStart == StorageConst.INDEX_BRANCH_START) {
//                        final byte[] newBuf1 = IndexPage.newBuffer(level);
//                        // 2. 写入值   最小值 直接写入第一页
//                        int _freeStart = writeIndexValue(lowerIndex, value, dataStart, newBuf1);
//                        updateFreeStart(_freeStart, newBuf1);
//                        page1 = persistence.writeIndex(tablePath, colName, newBuf1); // 第一页为新增的，返回地址相关信息
//                        toBeUpdated.set(i, new UpdatedIndex(page1, dataStart));
                        // 2. 写入值   最小值 直接写入第一页
                        page1 = (BranchIndexPage) persistence.newIndex(tablePath, colName, level);
                        page1.addIndexValue(lowerIndex, value, dataStart);
                        page1.save();
                        toBeUpdated.set(i, new UpdatedIndex(page1, dataStart));
                    } else { // 最大值和中间值 逻辑类似. 可能会有拷贝到第二页的数据为空的情况, 即参数5长度为0
//                        final byte[] newBuf2 = IndexPage.newBuffer(level);
//                        // 拷贝1后面的数据到2
//                        System.arraycopy(updateBuf, dataStart, newBuf2, StorageConst.INDEX_BRANCH_START, freeStart - dataStart);
//                        int freeStart1 = dataStart;
//                        int freeStart2 = StorageConst.INDEX_BRANCH_START + freeStart - dataStart;
//                        // 判断够不够写入的
//                        if (branchValCount <= freeEnd - freeStart2) { // 2够,写入2开头
//                            System.arraycopy(newBuf2, StorageConst.INDEX_BRANCH_START, newBuf2, StorageConst.INDEX_BRANCH_START + branchValCount, freeStart - dataStart);
//                            writeIndexValue(lowerIndex, value, StorageConst.INDEX_BRANCH_START, newBuf2);
//                            freeStart2 += branchValCount;
//                        } else if (branchValCount <= freeEnd - freeStart1) { // 1够,写入1末尾
//                            freeStart1 = writeIndexValue(lowerIndex, value, freeStart1, updateBuf);
//                        } else { // 都不够, 这种情况不可能, 因为会限制大于1000长度的字段禁止建立索引
//                            throw new RuntimeException("分页后仍然不够存储一个索引的, 暂时放弃治疗了");
//                        }
//                        // 更正1和2的头部
//                        updateFreeStart(freeStart1, updateBuf);
//                        updateFreeStart(freeStart2, newBuf2);
                        page2 = updatedPage.insertIndexValueNewly(lowerIndex, value, dataStart);
                        // 写入新页2 和 原页
//                        page2 = persistence.writeIndex(tablePath, colName, newBuf2);
//                        persistence.writeIndex(tablePath, colName, updatedIndex.getIndexPage());
                        page2.save();
                        updatedPage.save();
                        toBeUpdated.set(i, new UpdatedIndex(page2, page2.freeStart()));
                    }
                    if (i == toBeUpdated.size() - 1) {
                        // 当前是根节点了，需要升级 =======================================
                        BranchIndexPage rootPage = updatedIndex.getIndexPage();
                        if (page1 == null) { // 谁为空谁就是原根
//                            page1 = persistence.writeIndex(tablePath, colName, updateBuf); // 将原根写入其他位置, 把0号位置让给新根
                            // 将原根写入其他位置, 把0号位置让给新根
                            page1 = (BranchIndexPage) persistence.newIndex(tablePath, colName, level);
                            page1 = updatedPage.copyTo(page1.getFileId());
                        } else {
//                            page2 = persistence.writeIndex(tablePath, colName, updateBuf); // 将原根写入其他位置, 把0号位置让给新根
                            // 将原根写入其他位置, 把0号位置让给新根
                            page2 = (BranchIndexPage) persistence.newIndex(tablePath, colName, level);
                            page2 = updatedPage.copyTo(page2.getFileId());
                        }
//                        byte[] rootBuf = rootPage.getData();
//                        rootBuf[StorageConst.INDEX_LEVEL_START] = (byte)(level+1); // 写入level
//                        int len1 = ByteUtil.toShort(page1.getData(), StorageConst.INDEX_BRANCH_START);
//                        byte[] val1 = ArrayUtils.subarray(page1.getData(), StorageConst.INDEX_BRANCH_START + 2, StorageConst.INDEX_BRANCH_START + 2 + len1);
//                        int rootStart = writeIndexValue(page1, val1, StorageConst.INDEX_BRANCH_START, rootBuf); // 写入第一页的地址
                        // 写入第二页的地址
//                        int len2 = ByteUtil.toShort(page2.getData(), StorageConst.INDEX_BRANCH_START);
//                        byte[] val2 = ArrayUtils.subarray(page2.getData(), StorageConst.INDEX_BRANCH_START + 2, StorageConst.INDEX_BRANCH_START + 2 + len2);
//                        rootStart = writeIndexValue(page2, val2, rootStart, rootBuf);
//                        updateFreeStart(rootStart, rootBuf); // 更新新根的freeStart头信息
                        rootPage.level(level + 1);
                        final byte[] val1 = page1.minValue();
                        final int rootStart = rootPage.replaceIndexValue(page1, val1, StorageConst.INDEX_BRANCH_START);
                        // 写入第二页的地址
                        final byte[] val2 = page2.minValue();
                        rootPage.addIndexValue(page2, val2, rootStart);
                        // 保存新根
//                        persistence.writeIndex(tablePath, colName, rootPage);
                        rootPage.save();
                    }
                }
            }
        }
    }

    private void insertIndexFirst(String colName, DataPage dataPage, byte[] value) {
        // 先写入叶子节点
        LeafIndexPage leafPage = new LeafIndexPage(tablePath, colName, StorageConst.INDEX_DEF_FILE_ID, 1);
        leafPage.addIndexValue(dataPage, value);
        // --------------------------------------------------------------------------------
        // 再写入根索引
        BranchIndexPage rootPage = new BranchIndexPage(tablePath, colName, StorageConst.INDEX_DEF_FILE_ID);
        rootPage.level(1);
        rootPage.addIndexValue(leafPage, value);

        rootPage.save();
        leafPage.save();
    }

    public void updateIndexAddr(List<Map<DataPage.Row, Integer>> rowList, DataPage dataPage) {
        for (Constraint constraint : table.getConstraints()) {
            if (constraint.getType() != ConstraintType.PRIMARY_KEY
                    && constraint.getType() != ConstraintType.UNIQUE) {
                // TODO 暂只支持主键和唯一索引
                continue;
            }
            String[] colNames = constraint.getColumnNames();
            for (int i = 0; i < colNames.length; i++) {
                Column column = table.getColumn(colNames[i]);
                BranchIndexPage rootPage = (BranchIndexPage) persistence.readIndex(tablePath, colNames[i]);
                int idx = table.getColumnIndex(colNames[i]);
                for (Map<DataPage.Row, Integer> map : rowList) {
                    Map.Entry<DataPage.Row, Integer> entry = map.entrySet().iterator().next();
                    final DataPage.Row rr = entry.getKey();
                    final int newEnd = entry.getValue();
                    final byte[] value = tableData.valueToBytes(column, rr.data[idx], true);
                    final LinkedList<UpdatedIndex> toBeUpdated = new LinkedList<>();
                    LeafIndexPage leafPage = rootPage.findLeafIndex(column, value, toBeUpdated);
                    IndexPage.LeafResult lr = leafPage.findLeafStart(column, value);
                    if (lr.isNew) {
                        continue; // 没找到，不处理了
                    }
                    final byte[] dataAddr = dataPage.toAddress(rr); // 8位地址数据
                    final IndexPage.LeafResult addrResult = leafPage.findAddressStart(dataAddr, lr.dataStart);
                    if (addrResult.leafPage == null) { // 没找到
                        continue;
                    }
                    leafPage = addrResult.leafPage;
                    // 直接写入覆盖旧的地址值
                    dataPage.setLocation(new Location((short) rr.start, (short) (newEnd - rr.start)));
                    leafPage.replaceIndexValue(dataPage, addrResult.dataStart);
                    persistence.writeIndex(tablePath, colNames[i], leafPage);
                }
            }
        }
    }

    public void deleteIndex(List<DataPage.Row> rowList, DataPage dataPage) {
        for (Constraint constraint : table.getConstraints()) {
            if (constraint.getType() != ConstraintType.PRIMARY_KEY
                    && constraint.getType() != ConstraintType.UNIQUE) {
                // TODO 暂只支持主键和唯一索引
                continue;
            }
            String[] colNames = constraint.getColumnNames();
            for (int i = 0; i < colNames.length; i++) {
                Column column = table.getColumn(colNames[i]);
                BranchIndexPage rootPage = (BranchIndexPage) persistence.readIndex(tablePath, colNames[i]);
                int idx = table.getColumnIndex(colNames[i]);
                for (DataPage.Row row : rowList) {
                    final byte[] value = tableData.valueToBytes(column, row.data[idx], true);
                    final LinkedList<UpdatedIndex> toBeUpdated = new LinkedList<>();
                    LeafIndexPage leafPage = rootPage.findLeafIndex(column, value, toBeUpdated);
//                    byte[] leafBuf = leafPage.getData();
//                    int freeStart = leafPage.freeStart();
                    final IndexPage.LeafResult lr = leafPage.findLeafStart(column, value);
                    if (lr.isNew) {
                        continue; // 没找到，不处理了
                    }
                    final byte[] dataAddr = dataPage.toAddress(row); // 8位地址数据
                    final IndexPage.LeafResult addrResult = leafPage.findAddressStart(dataAddr, lr.dataStart);
//                    final int addrLen = leafPage.addressLength(dataStart - 4);
//                    int loop = 0;
//                        // 没有跳页
//                    byte[] dataAddr2;
//                    boolean notFound = false;
//                    do {
//                        if (loop++ >= addrLen) {
//                            notFound = true;
//                            break; // 遍历到头了还没找到
//                        }
//                        if (dataStart >= freeStart) {
//                            leafPage = leafPage.next();
//                            freeStart = leafPage.freeStart();
//                            dataStart = StorageConst.INDEX_LEAF_START; // next page的开头直接存储地址值
//                        }
//                        dataAddr2 = leafPage.address(dataStart);
//                        dataStart += 8;
//                    } while (!Arrays.equals(dataAddr, dataAddr2));
                    if (addrResult.leafPage == null) { // 没找到
                        continue;
                    }
                    leafPage = addrResult.leafPage;
                    final boolean needUpdateBranch = leafPage.deleteOne(value, addrResult.dataStart, addrResult.addressLength);
//                    dataStart = addrResult.dataStart;
//                    freeStart = leafPage.freeStart();
//                    int _len = 0;
//                    boolean needUpdateBranch = false;
//                    if (addrResult.addressLength == 1) { // 只有一个长度, 需要连数据也一起删掉
//                        _len = 4 + value.length + 2; // 一条索引数据的总长度(不算索引地址的长度)
//                        if (dataStart - _len == StorageConst.INDEX_LEAF_START) {
//                            needUpdateBranch = true;
//                            // 是最小值了，需要同时删除树枝索引的最小值
//                        }
//                    }
//                    // 从索引中删除该地址
//                    System.arraycopy(leafBuf, dataStart, leafBuf, dataStart - _len, freeStart - dataStart);
//                    updateFreeStart(freeStart - _len, leafBuf);
//                    persistence.writeIndex(tablePath, colNames[i], leafPage);
                    if (needUpdateBranch) {
                        updateBranchIndex(column.getName(), leafPage.minValue(), UpdateBranchType.UPDATE, toBeUpdated, leafPage);
                    }
                }
            }
        }
    }

}
