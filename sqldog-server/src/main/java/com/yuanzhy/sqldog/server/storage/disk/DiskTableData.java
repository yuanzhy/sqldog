package com.yuanzhy.sqldog.server.storage.disk;

import com.yuanzhy.sqldog.core.util.ArrayUtils;
import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.core.util.ByteUtil;
import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.common.collection.PrimitiveByteList;
import com.yuanzhy.sqldog.server.common.model.DataPage;
import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.core.Constraint;
import com.yuanzhy.sqldog.server.core.Persistence;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.core.TableData;
import com.yuanzhy.sqldog.server.core.constant.ConstraintType;
import com.yuanzhy.sqldog.server.storage.base.AbstractTableData;
import com.yuanzhy.sqldog.server.storage.persistence.PersistenceFactory;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUpdate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * 存储设计：
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/4
 */
public class DiskTableData extends AbstractTableData implements TableData {

    private final Persistence persistence;
    private final String tablePath;

    private final DiskTableIndex tableIndex;

    private volatile int count;
    private volatile boolean optimizing = false;
    DiskTableData(Table table, String tablePath) {
        super(table);
        this.tablePath = tablePath;
        this.persistence = PersistenceFactory.get();
        tableIndex = new DiskTableIndex(table, tablePath, this);
        // 初始化统计信息
        Map<String, Object> statMap = persistence.readStatistics(tablePath);
        if (statMap.containsKey("count")) {
            this.count = (int) statMap.get("count");
        } else {
            // correct  这样实现不太好，先这样吧
            this.count = this.getData().size();
            this.saveStatistics();
        }
    }
    @Override
    public synchronized Object[] insert(Map<String, Object> values) {
        // check
        this.checkData(values);
        // generate pk
        Object[] pkValues = generatePkValues(values);
        // check pk
        if (pkValues != null) {
            String[] pkNames = table.getPkColumnName();
            byte[][] bytes = new byte[pkValues.length][];
            for (int i = 0; i < pkValues.length; i++) {
                bytes[i] = valueToBytes(table.getColumn(pkNames[i]), pkValues[i], true);
            }
            Asserts.isFalse(tableIndex.isConflict(pkNames, bytes), "Primary key conflict：" + Arrays.stream(pkValues).map(Object::toString).collect(Collectors.joining(", ")));
        }

        // check constraint
        for (Constraint c : table.getConstraints()) {
            String[] columnNames = c.getColumnNames();
            if (c.getType() == ConstraintType.UNIQUE) {
                byte[][] bytes = new byte[columnNames.length][];
                for (int i = 0; i < columnNames.length; i++) {
                    bytes[i] = valueToBytes(table.getColumn(columnNames[i]), values.get(columnNames[i]), true);
                }
                if (tableIndex.isConflict(columnNames, bytes)) {
                    String columnValues = Arrays.stream(columnNames).map(n -> Objects.toString(values.get(n))).collect(Collectors.joining(", "));
                    throw new IllegalArgumentException("Unique key conflict：" + columnValues);
                }
            }
        }
        // add data
        Map<String, Object> row = normalizeData(values);
        /*DataPage dataPage = */this.insertData(row);
        count++;
        saveStatistics();
        return pkValues;
    }

    private void saveStatistics() {
        // TODO 后续可能会有其他统计信息
        persistence.writeStatistics(tablePath, Collections.singletonMap("count", count));
    }

    /**
     *
     * @param column 列信息
     * @param value  值
     * @return byte数组，如果是char,varchar,decimal,number类型则包含长度
     */
    byte[] valueToBytes(Column column, Object value, boolean toIndex) {
        switch (column.getDataType()) {
            case INT:
            case SERIAL:
                return ByteUtil.toBytes(((Number) value).intValue());
            case BIGINT:
            case BIGSERIAL:
                return ByteUtil.toBytes(((Number) value).longValue());
            case SMALLINT:
            case SMALLSERIAL:
                return ByteUtil.toBytes(((Number) value).shortValue());
            case TINYINT:
                return new byte[]{((Number) value).byteValue()};
            case FLOAT:
                return ByteUtil.toBytes(((Number) value).floatValue());
            case DOUBLE:
                return ByteUtil.toBytes(((Number) value).doubleValue());
            case TIME:
//                int iv;
//                if (value instanceof Date) {
//                    iv = (int)((Date)value).getTime();
//                } else if (value instanceof Number) {
//                    iv = ((Number)value).intValue();
//                } else {
//                    iv = Integer.parseInt(value.toString());
//                }
//                return ByteUtil.toBytes(iv);
            case DATE:
            case TIMESTAMP:
                long v;
                if (value instanceof Date) {
                    v = ((Date)value).getTime();
                } else if (value instanceof Number) {
                    v = ((Number)value).longValue();
                } else {
                    v = Long.parseLong(value.toString());
                }
                return ByteUtil.toBytes(v);
            case BOOLEAN:
                byte b = (byte)(((Boolean)value) ? 0b1 : 0b0);
                return new byte[]{b};
            case BYTEA:
            case TEXT:
            case VARCHAR: // varchar 超过1000的情况也挺多的，此处和大字段一样的处理吧
                byte[] bytes = (value instanceof byte[]) ? (byte[]) value : ByteUtil.toBytes(value.toString());
                if (bytes.length > StorageConst.LARGE_FIELD_THRESHOLD) {
                    // 需要存储在外部, 此处只存储代表外部存储的标识
                    byte[] extraId = persistence.writeExtraData(tablePath, bytes);
                    return extraId;
                } else {
                    // 存储在数据内部，和varchar逻辑一致
                    if (toIndex) {
                        return bytes;
                    }
                    short len = (short)bytes.length;
                    return ArrayUtils.addAll(ByteUtil.toBytes(len), bytes);
                }
            case ARRAY:
            case JSON:
                throw new UnsupportedOperationException("暂未实现大字段存储");
            default: // CHAR, DECIMAL, NUMERIC
                byte[] strBytes = ByteUtil.toBytes(value.toString());
                if (toIndex) {
                    return strBytes;
                }
                short len = (short)strBytes.length;
                return ArrayUtils.addAll(ByteUtil.toBytes(len), strBytes);
        }
    }

    /**
     * 内部插入，参数格式已经校验和规范化，不需要再处理了
     * @param row normalizeData
     */
    private DataPage insertData(Map<String, Object> row) {
        PrimitiveByteList dataBytes = transformToBinary(row);
        DataPage dataPage = persistence.getInsertablePage(tablePath);
        dataPage = dataPage.insert(dataBytes).save();

        // insert pk index
        String[] pkNames = table.getPkColumnName();
        if (pkNames != null) {
            Set<String> colNameSet = table.getColumns().keySet();
            Object[] pkValues = new Object[pkNames.length];
            int pkIndex = 0;
            for (String colName : colNameSet) {
                if (ArrayUtils.contains(pkNames, colName)) {
                    pkValues[pkIndex++] = row.get(colName);
                }
            }
            for (int i = 0; i < pkNames.length; i++) {
                Column col = table.getColumn(pkNames[i]);
                tableIndex.insertIndex(col, valueToBytes(col, pkValues[i], true), dataPage);
            }
        }
        // TODO insert unique index and other

        return dataPage;
    }

    /**
     * 数据转换为二进制
     * @param row 一行数据
     * @return
     */
    private PrimitiveByteList transformToBinary(Map<String, Object> row) {
        PrimitiveByteList dataList = new PrimitiveByteList();
        int nullBytesCount = getNullBytesCount();
        byte[] nullFlags = new byte[nullBytesCount];
        int nullIdx = 0;
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            Column column = table.getColumn(entry.getKey());
            Object value = entry.getValue();
            if (column.isNullable()) {
                if (value == null) {
                    nullFlags[nullIdx / 8] |= (byte) Math.pow(2, nullIdx % 8);
                    ++nullIdx;
                    continue;
                } else {
                    ++nullIdx;
                }
            }
            dataList.addAll(valueToBytes(column, value, false));
        }
        dataList.addFirst(nullFlags);
        short dataHeader = (short)dataList.size();
        dataList.addFirst(ByteUtil.toBytes(dataHeader));
        // Data Header + Data
        return dataList;
    }

    @Override
    public synchronized int deleteBy(SqlDelete sqlDelete) {
        int _count = 0;
        SqlNode condition = sqlDelete.getCondition();
        if (condition == null) {
            _count = count;
            this.truncate();
            return _count;
        } else if (condition instanceof SqlBasicCall) {
            final int nullBytesCount = getNullBytesCount();
            // 判断是不是简单条件， 比如 where ID = '1'
            boolean simpleCondition = false;
            if (simpleCondition) { //
                // TODO 基于索引删除的情况再议，等查询优化完成后在弄吧
            } else { // 没有索引，全表扫描的删吧
                Predicate<Object[]> predicate = handleWhere((SqlBasicCall)condition);
                DataPage dataPage = persistence.readPage(tablePath);
                while (dataPage != null) {
                    _count += deleteByCondition(dataPage, nullBytesCount, predicate);
                    dataPage = persistence.readPage(tablePath, dataPage.getFileId(), dataPage.getOffset() + 1);
                }
            }
        } else {
            throw new UnsupportedOperationException("not supported: " + condition);
        }
        // 更新总数
        if (_count > 0) {
            count -= _count;
            this.saveStatistics();
            if (count == 0) {
                this.truncate(); // 当作优化表碎片了
            }
        }
        return _count;
    }

    @Override
    public synchronized int updateBy(SqlUpdate sqlUpdate) {
        final List<String> colList = sqlUpdate.getTargetColumnList().stream().map(SqlNode::toString).collect(Collectors.toList());
        if (table.getPrimaryKey() != null) {
            for (String columnName : table.getPrimaryKey().getColumnNames()) { // TODO 更新主键字段
                Asserts.isFalse(colList.contains(columnName), "Temporary unsupported update primary key");
            }
        }
        for (Constraint c : table.getConstraints()) {
            if (c.getType() == ConstraintType.UNIQUE) {
                for (String columnName : c.getColumnNames()) { // TODO 更新唯一字段
                    Asserts.isFalse(colList.contains(columnName), "Temporary unsupported update unique key");
                }
            }
        }
        int _count = 0;
        List<Object> valList = new ArrayList<>(colList.size());
        for (int i = 0; i < colList.size(); i++) {
            Column column = table.getColumn(colList.get(i));
            SqlNode s = sqlUpdate.getSourceExpressionList().get(i);
            if (!(s instanceof SqlLiteral)) {
                throw new UnsupportedOperationException("not supported: " + s.toString());
            }
            Object val = parseValue(s, column.getDataType());
            if (!column.isNullable() && val == null) {
                throw new IllegalArgumentException("'" + column.getName() + "' is not null");
            }
            val = checkVal(column, val);
            valList.add(val);
        }

        SqlNode condition = sqlUpdate.getCondition();
        Predicate<Object[]> predicate;
        if (condition == null) {
//            predicate = (o) -> true;
            // 更新所有的情况单独处理一下，不然索引处可能会出现问题
            this.updateAll(colList, valList);
            return count;
        } else if (condition instanceof SqlBasicCall) { // TODO 基于索引的更新等查询优化后再实现
            predicate = handleWhere((SqlBasicCall) condition);
        } else {
            throw new UnsupportedOperationException("not supported: " + condition);
        }
        DataPage dataPage = persistence.readPage(tablePath);
        int nullBytesCount = getNullBytesCount();
        while (dataPage != null) {
            _count += updateByCondition(dataPage, nullBytesCount, predicate, colList, valList);
            dataPage = persistence.readPage(tablePath, dataPage.getFileId(), dataPage.getOffset() + 1);
        }
        return _count;
    }

    private int updateByCondition(DataPage dataPage, int nullBytesCount, Predicate<Object[]> predicate, List<String> colList, List<Object> valList) {
        int _count = 0;
//        final byte[] pageBuf = dataPage.getData();
        final Collection<Column> columns = table.getColumns().values();
        int dataStart = StorageConst.DATA_START_OFFSET;
        final short freeStart = dataPage.freeStart();
//        final int freeEnd = dataPage.freeEnd();
        List<DataPage.Row> deletedDatas = new ArrayList<>();
        List<Map<String, Object>> insertedDatas = new ArrayList<>();
        List<Map<DataPage.Row, Integer>> updatedDatas = new ArrayList<>();
        while (dataStart < freeStart) {  // 读取多行数据
            DataPage.Row row = dataPage.row(columns, nullBytesCount, dataStart);
            dataStart = row.end;
            if (row.data == null) { // 说明是删除的记录
                continue;
            }
            if (predicate.test(row.data)) { // 符合更新条件，弄它
                Map<String, Object> newRow = toMap(row.data, columns);
                for (int i = 0; i < colList.size(); i++) {
                    String colName = colList.get(i);
                    Object val = valList.get(i);
                    newRow.put(colName, val);
                }
                PrimitiveByteList newRowBytes = transformToBinary(newRow);
                if (newRowBytes.size() == row.end - row.start) { // update后长度没变，直接复用原来的位置. 索引也不用更新了
                    dataPage.replace(row.start, newRowBytes);
                }
//                else if (newRowBytes.size() <= freeEnd - freeStart) {
//                    // 一页能放下就动态调节 TODO 动态调节这个比较复杂，再议。调节完后面数据的索引就都得调整
//                    int _start = rr.start;
//                    final int newEnd = _start + newRowBytes.size();
//                    System.arraycopy(pageBuf, rr.end, pageBuf, newEnd, freeStart - rr.end);
//                    for (Byte dataByte : newRowBytes) {
//                        pageBuf[_start++] = dataByte;
//                    }
//                    int _freeStart = freeStart + newRowBytes.size() - (rr.end - rr.start);
//                    updateFreeStart(_freeStart, pageBuf);
//                    updatedDatas.add(Collections.singletonMap(rr, newEnd));
//                    dataStart = newEnd;
//                }
                else {
                    // 删除原来的 插入一个新的
                    dataPage.delete(row.start);
                    deletedDatas.add(row);
                    // 新增一条记录
                    insertedDatas.add(newRow);
                }
                _count++;
            }
        }
        if (_count > 0) {
            dataPage.save();
//            if (!updatedDatas.isEmpty()) {
//                tableIndex.updateIndexAddr(updatedDatas,  dataPage);
//            }
            if (!deletedDatas.isEmpty()) {
                tableIndex.deleteIndex(deletedDatas, dataPage);
            }
            for (Map<String, Object> insertedData : insertedDatas) {
                this.insertData(insertedData);
            }
        }
        return _count;
    }

    private Map<String, Object> toMap(Object[] row, Collection<Column> columns) {
        int i = 0;
        Map<String, Object> map = new LinkedHashMap<>();
        for (Column column : columns) {
            map.put(column.getName(), row[i++]);
        }
        return map;
    }

    @Override
    public synchronized void truncate() {
        persistence.delete(persistence.resolvePath(tablePath, StorageConst.TABLE_DATA_PATH));
        persistence.delete(persistence.resolvePath(tablePath, StorageConst.TABLE_INDEX_PATH));
        this.count = 0;
        this.saveStatistics();
    }

    @Override
    @Deprecated
    public List<Object[]> getData() {
        checkTableState();
        final int nullBytesCount = getNullBytesCount();
        List<Object[]> data = new ArrayList<>();
        // 读取临时目录的旧数据，向正式目录写入新数据
        DataPage dataPage = persistence.readPage(tablePath);
        while (dataPage != null) {
            data.addAll(transformToData(dataPage, nullBytesCount));
            dataPage = persistence.readPage(tablePath, dataPage.getFileId(), dataPage.getOffset() + 1);
        }
        return data;
    }

    @Override
    public int getCount() {
        return count;
    }

    private int getNullBytesCount() {
        int nullableCount = (int)table.getColumns().values().stream().filter(Column::isNullable).count();
        return nullableCount % 8 > 0 ? nullableCount / 8 + 1 : nullableCount / 8;
    }

    private void checkTableState() {
        if (optimizing) {
            throw new IllegalStateException("Table Optimizing ...");
        }
    }

//    private List<Map<String, Object>> transformToMap(byte[] pageBuf, int nullBytesCount) {
//        String[] colNames = table.getColumns().keySet().toArray(new String[0]);
//        return transformToData(pageBuf, nullBytesCount).stream().map(o -> {
//            Map<String, Object> m = new LinkedHashMap<>();
//            for (int i = 0; i < colNames.length; i++) {
//                m.put(colNames[i], o[i]);
//            }
//            return m;
//        }).collect(Collectors.toList());
//    }

    /**
     * 删除一页中符合条件的记录
     * @param dataPage        数据页
     * @param nullBytesCount  null字段byte数
     * @param predicate       条件
     * @return 删除数量
     */
    private int deleteByCondition(DataPage dataPage, int nullBytesCount, Predicate<Object[]> predicate) {
//        final byte[] pageBuf = dataPage.getData();
        final Collection<Column> columns = table.getColumns().values();
        int dataStart = StorageConst.DATA_START_OFFSET;
        short freeStart = dataPage.freeStart();
        List<DataPage.Row> deletedDatas = new ArrayList<>();
        while (dataStart < freeStart) {  // 读取多行数据
            DataPage.Row row = dataPage.row(columns, nullBytesCount, dataStart);
            dataStart = row.end;
            if (row.data == null) { // 说明是删除的记录
                continue;
            }
            if (predicate.test(row.data)) { // 符合删除条件，干掉它
                dataPage.delete(row.start);
                deletedDatas.add(row);
            }
        }
        if (!deletedDatas.isEmpty()) { // 有删除的情况，持久化以下
//            persistence.writePage(tablePath, dataPage);
            dataPage.save();
            // 同时删除索引
            tableIndex.deleteIndex(deletedDatas, dataPage);
        }
        return deletedDatas.size();
    }

    private List<Object[]> transformToData(DataPage dataPage, int nullBytesCount) {
        final List<Object[]> data = new ArrayList<>();
        final Collection<Column> columns = diskTable().getOldColumns().values();
        int dataStart = StorageConst.DATA_START_OFFSET;
        final short freeStart = dataPage.freeStart();
        while (dataStart < freeStart) {  // 读取多行数据
            DataPage.Row row = dataPage.row(columns, nullBytesCount, dataStart);
            dataStart = row.end;
            if (row.data == null) { // 说明是删除的记录
                continue;
            }
            data.add(row.data);
        }
        return data;
    }

    @Override
    public synchronized void addColumn(Column column) {
        optimizing = true;
        try {
            this.alterColumn(column, -1);
        } finally {
            optimizing = false;
        }
    }

    @Override
    public synchronized void dropColumn(Column column, int deleteIndex) {
        optimizing = true;
        try {
            this.alterColumn(column, deleteIndex);
        } finally {
            optimizing = false;
        }
    }

    @Override
    public void optimize() {
        optimizing = true;
        try {
            this.alterColumn(null, -1);
        } finally {
            optimizing = false;
        }
    }

    private void updateAll(List<String> colList, List<Object> valList) {
        DataPage dataPage = persistence.readPage(tablePath);
//        DataExtent oldDataExtend = persistence.readExtent(tablePath);
        if (dataPage == null) return; // 没有数据，直接返回
        Set<String> colNameSet = diskTable().getColumns().keySet();
        int nullBytesCount = getNullBytesCount();
        String tmpTablePath = persistence.resolvePath(tablePath, "_upt");
        // 旧数据移入临时目录
        persistence.move(tablePath, tmpTablePath);
        // 删除旧索引信息
        persistence.delete(persistence.resolvePath(tablePath, StorageConst.TABLE_INDEX_PATH));
        // 读取临时目录的旧数据，向正式目录写入新数据
        dataPage = persistence.readPage(tmpTablePath);

        while (dataPage != null) {
            List<Object[]> data = transformToData(dataPage, nullBytesCount);
            for (Object[] d : data) {
                int i = 0;
                Map<String, Object> newRow = new LinkedHashMap<>();
                for (String colName : colNameSet) {
                    newRow.put(colName, d[i++]);
                }
                for (int j = 0; j < colList.size(); j++) {
                    String colName = colList.get(j);
                    Object val = valList.get(j);
                    newRow.put(colName, val);
                }
                this.insertData(newRow);
            }
            dataPage = persistence.readPage(tablePath, dataPage.getFileId(), dataPage.getOffset() + 1);
        }
        persistence.delete(tmpTablePath);
    }

    private void alterColumn(Column column, int deleteIndex) {
        DataPage dataPage = persistence.readPage(tablePath);
//        DataExtent oldDataExtend = persistence.readExtent(tablePath);
        if (dataPage == null) return; // 没有数据，直接返回
        Set<String> colNameSet = diskTable().getOldColumns().keySet();
        int nullableCount = (int)diskTable().getOldColumns().values().stream().filter(Column::isNullable).count();
        int nullBytesCount = nullableCount % 8 > 0 ? nullableCount / 8 + 1 : nullableCount / 8;
        String[] pkNames = table.getPkColumnName();
        String tmpTablePath = persistence.resolvePath(tablePath, "tmp");
        // 旧数据移入临时目录
        persistence.move(tablePath, tmpTablePath);
        // 删除旧索引信息
        persistence.delete(persistence.resolvePath(tablePath, StorageConst.TABLE_INDEX_PATH));
        // 读取临时目录的旧数据，向正式目录写入新数据
        dataPage = persistence.readPage(tmpTablePath);
        Map<String, Object> row = new LinkedHashMap<>();
        boolean isAdd = column != null && deleteIndex < 0;
        while (dataPage != null) {
            List<Object[]> data = transformToData(dataPage, nullBytesCount);
            for (Object[] d : data) {
                int i = 0;
                for (String colName : colNameSet) {
                    if (deleteIndex == i) {
                        i++;
                        continue;
                    }
                    row.put(colName, d[i++]);
                }
                if (isAdd) {
                    row.put(column.getName(), column.defaultValue());
                }
                this.insertData(row);
            }
            dataPage = persistence.readPage(tablePath, dataPage.getFileId(), dataPage.getOffset() + 1);
        }
        persistence.delete(tmpTablePath);
    }

    private DiskTable diskTable() {
        return (DiskTable) table;
    }

    @Override
    public Iterator<Object[]> iterator() {
        return new DiskDataIterator();
    }

    private class DiskDataIterator implements Iterator<Object[]>, AutoCloseable {

        private final int nullBytesCount;
        private boolean closed = false;
        private int globalIndex = -1;
        private int pageIndex;
        private DataPage dataPage;
        private List<Object[]> pageData;

        DiskDataIterator() {
            nullBytesCount = getNullBytesCount();
            nextData();
            pageIndex = -1;
        }
        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            if (++globalIndex >= count) {
                this.close();
                return false;
            }
            if (++pageIndex < pageData.size()) {
                return true;
            }
            return nextData();
        }

        @Override
        public Object[] next() {
            if (closed) {
                throw new NoSuchElementException(table.getName());
            }
            return pageData.get(pageIndex);
        }

        @Override
        public void close() {
            dataPage = null;
            pageData = null;
            closed = true;
        }

        private boolean nextData() {
            checkTableState();
            if (dataPage == null) {
                dataPage = persistence.readPage(tablePath);
            } else {
                dataPage = persistence.readPage(tablePath, dataPage.getFileId(), dataPage.getOffset() + 1);
            }
            if (dataPage == null) {
                close();
                return false;
            } else {
                pageData = transformToData(dataPage, nullBytesCount);
                if (pageData.isEmpty()) { // 最虽的情况，一页数据都被删除了
                    return nextData();
                } else {
                    pageIndex = 0;
                    return true;
                }
            }
        }
    }
}
