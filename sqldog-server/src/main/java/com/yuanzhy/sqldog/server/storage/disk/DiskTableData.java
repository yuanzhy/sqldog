package com.yuanzhy.sqldog.server.storage.disk;

import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.common.model.DataExtent;
import com.yuanzhy.sqldog.server.common.model.DataPage;
import com.yuanzhy.sqldog.server.common.model.Location;
import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.core.Constraint;
import com.yuanzhy.sqldog.server.core.Persistence;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.core.TableData;
import com.yuanzhy.sqldog.server.core.constant.ConstraintType;
import com.yuanzhy.sqldog.server.core.constant.DataType;
import com.yuanzhy.sqldog.server.storage.base.AbstractTableData;
import com.yuanzhy.sqldog.server.storage.persistence.PersistenceFactory;
import com.yuanzhy.sqldog.server.util.ByteUtil;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.commons.lang3.ArrayUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private volatile boolean optimizing = false;
    DiskTableData(Table table, String tablePath) {
        super(table);
        this.tablePath = tablePath;
        this.persistence = PersistenceFactory.get();
        tableIndex = new DiskTableIndex(table, tablePath);
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
                bytes[i] = valueToBytes(table.getColumn(pkNames[i]), pkValues[i]);
            }
            Asserts.isFalse(tableIndex.isConflict(pkNames, bytes), "Primary key conflict：" + Arrays.stream(pkValues).map(Object::toString).collect(Collectors.joining(", ")));
        }

        // check constraint
        for (Constraint c : table.getConstraints()) {
            String[] columnNames = c.getColumnNames();
            if (c.getType() == ConstraintType.UNIQUE) {
                byte[][] bytes = new byte[columnNames.length][];
                for (int i = 0; i < columnNames.length; i++) {
                    bytes[i] = valueToBytes(table.getColumn(columnNames[i]), values.get(columnNames[i]));
                }
                // TODO unique冲突提醒优化
                Asserts.isFalse(tableIndex.isConflict(columnNames, bytes), "Unique key conflict：" + Arrays.stream(columnNames).map(Object::toString).collect(Collectors.joining(", ")));
            }
        }
        // add data
        Map<String, Object> row = normalizeData(values);
        DataPage dataPage = this.insertData(row);
        // add index
        if (pkValues != null) {
            String[] colNames = table.getPkColumnName();
            for (int i = 0; i < colNames.length; i++) {
                Column column = table.getColumn(colNames[i]);
                tableIndex.insertIndex(column, valueToBytes(column, pkValues[i]), dataPage);
            }
        }
        return pkValues;
    }

    private byte[] valueToBytes(Column column, Object value) {
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
            case DATE:
            case TIMESTAMP:
            case TIME:
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
                byte[] bytes = (value instanceof byte[]) ? (byte[]) value : ByteUtil.toBytes(value.toString());
                if (bytes.length > StorageConst.LARGE_FIELD_THRESHOLD) {
                    // 需要存储在外部, 此处只存储代表外部存储的标识
                    byte[] extraId = persistence.writeExtraData(tablePath, bytes);
                    return extraId;
                } else {
                    // 存储在数据内部，和varchar逻辑一致
                    short len = (short)bytes.length;
                    return ArrayUtils.addAll(ByteUtil.toBytes(len), bytes);
                }
            case ARRAY:
            case JSON:
                throw new UnsupportedOperationException("暂未实现大字段存储");
            default: // VARCHAR, CHAR, DECIMAL, NUMERIC
                byte[] strBytes = ByteUtil.toBytes(value.toString());
                short len = (short)strBytes.length;
                return ArrayUtils.addAll(ByteUtil.toBytes(len), strBytes);
        }
    }

    /**
     * 内部插入，参数格式已经校验和规范化，不需要再处理了
     * @param row normalizeData
     */
    private DataPage insertData(Map<String, Object> row) {
        List<Byte> dataBytes = transformToBinary(row);
        DataPage dataPage = persistence.getInsertablePage(tablePath);
        byte[] pageBuf = dataPage.getData();
        short freeStart = ByteUtil.toShort(pageBuf, StorageConst.FREE_START_OFFSET);
        short freeEnd = ByteUtil.toShort(pageBuf, StorageConst.FREE_END_OFFSET);
        // ------- 写入数据 -------
        if (dataBytes.size() >= StorageConst.PAGE_SIZE - 16) {
            // TODO
            throw new RuntimeException("单条记录大于一页的情况暂未实现：" + dataBytes.size());
        }
        if (freeEnd - freeStart < dataBytes.size()) {  // 剩余page空间不够存储本条记录
            // 生成一个新的 page 追加到文件末尾，这里还复用pageBuf减少垃圾回收压力。虽然后面的数据还是上一页的，但是FREE_START等标志位是正确的
            Location loction = fillPageBuf(dataBytes, pageBuf, StorageConst.DATA_START_OFFSET, StorageConst.PAGE_SIZE);
            // dataPage.getOffset() + StorageConst.PAGE_SIZE == file.length
            DataPage newDataPage = new DataPage(dataPage.getFileId(), dataPage.getOffset() + 1, pageBuf);
            dataPage = persistence.writePage(tablePath, newDataPage);
            dataPage.setLocation(loction);
        } else { // page 空间够用, 在当前 page 空闲区写入数据
            Location loction = fillPageBuf(dataBytes, pageBuf, freeStart, freeEnd);
            // 将当前 page 回写文件
            dataPage = persistence.writePage(tablePath, dataPage);
            dataPage.setLocation(loction);
        }
        return dataPage;

    }

    private Location fillPageBuf(List<Byte> dataBytes, byte[] pageBuf, short freeStart, short freeEnd) {
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
        for (Byte dataByte : dataBytes) {
            pageBuf[freeStart++] = dataByte;
        }
        // 更新freeStart字段
        byte[] startBytes = ByteUtil.toBytes(freeStart);
        pageBuf[4] = startBytes[0];
        pageBuf[5] = startBytes[1];
        return new Location((short)locStart, (short)(freeStart - locStart));
    }

    /**
     * 数据转换为二进制
     * @param row 一行数据
     * @return
     */
    private List<Byte> transformToBinary(Map<String, Object> row) {
        LinkedList<Byte> dataList = new LinkedList<>();
        int nullableCount = (int)table.getColumns().values().stream().filter(Column::isNullable).count();
        int nullBytesCount = nullableCount % 8 > 0 ? nullableCount / 8 + 1 : nullableCount / 8;
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
            addAll(dataList, valueToBytes(column, value));
        }
        addFirst(dataList, nullFlags);
        short dataHeader = (short)dataList.size();
        addFirst(dataList, ByteUtil.toBytes(dataHeader));
        // Data Header + Data
        return dataList;
    }



    private void addFirst(List<Byte> list, byte[] bytes) {
        for (int i = bytes.length - 1; i >= 0; i--) {
            list.add(0, bytes[i]);
        }
    }
    private void addAll(List<Byte> list, byte[] bytes) {
        for (byte b : bytes) {
            list.add(b);
        }
    }

    @Override
    public synchronized int deleteBy(SqlDelete sqlDelete) {
        int count = 0;
//        SqlNode condition = sqlDelete.getCondition();
//        if (condition == null) {
//            count = data.size();
//            this.truncate();
//        } else if (condition instanceof SqlBasicCall) {
//            Set<Map<String, Object>> dataList = handleWhere(data, (SqlBasicCall)condition);
//            count = dataList.size();
//            String[] pkNames = table.getPkColumnName();
//            for (Map<String, Object> row : dataList) {
//                // 删除主键索引
//                if (pkNames != null) {
//                    pkSet.remove(uniqueColValue(pkNames, row));
//                }
//                // 删除唯一索引
//                for (Constraint c : table.getConstraints()) {
//                    String[] columnNames = c.getColumnNames();
//                    if (c.getType() == ConstraintType.UNIQUE) {
//                        uniqueMap.getOrDefault(uniqueColName(columnNames), Collections.emptySet()).remove(uniqueColValue(columnNames, row));
//                    }
//                }
//            }
//            data.removeAll(dataList);
//        } else {
//            throw new UnsupportedOperationException("not supported: " + condition);
//        }
        return count;
    }

    @Override
    public synchronized int updateBy(SqlUpdate sqlUpdate) {
        return 0;
    }

    @Override
    public synchronized void truncate() {
        persistence.delete(persistence.resolvePath(tablePath, StorageConst.TABLE_DATA_PATH));
        persistence.delete(persistence.resolvePath(tablePath, StorageConst.TABLE_INDEX_PATH));
    }

    @Override
    @Deprecated
    public List<Object[]> getData() {
        checkTableState();
        Collection<Column> columns = table.getColumns().values();
        int nullableCount = (int)columns.stream().filter(Column::isNullable).count();
        int nullBytesCount = nullableCount % 8 > 0 ? nullableCount / 8 + 1 : nullableCount / 8;

        List<Object[]> data = new ArrayList<>();
        DataExtent dataExtent = persistence.readExtent(tablePath);
        while (dataExtent != null) {
            int pageOffset = 0;
            byte[] pageBuf;
            while ((pageBuf = dataExtent.getPage(pageOffset++)) != null) {
                data.addAll(transformToData(pageBuf, nullBytesCount));
            }
            dataExtent = persistence.readExtent(tablePath, dataExtent.getFileId(), dataExtent.getOffset() + 1);
        }
        return data;
    }

    private void checkTableState() {
        if (optimizing) {
            throw new IllegalStateException("Table Optimizing ...");
        }
    }

    private List<Object[]> transformToData(byte[] pageBuf, int nullBytesCount) {
        List<Object[]> data = new ArrayList<>();
        Collection<Column> columns = diskTable().getOldColumns().values();
        int dataStart = StorageConst.DATA_START_OFFSET;
        short freeStart = ByteUtil.toShort(pageBuf, StorageConst.FREE_START_OFFSET);
        while (dataStart < freeStart) {  // 读取多行数据
            short dataHeader = ByteUtil.toShort(new byte[]{pageBuf[dataStart++], pageBuf[dataStart++]});
            boolean deleted = ((dataHeader >> 15) & 0b1) == 1;
            if (deleted) continue;
            int end = dataStart + (dataHeader & 0b0111111111111111);
            // 读取nullFlag
            byte[] nullFlags = new byte[nullBytesCount];
//            for (int i = nullBytesCount - 1; i >= 0; i--) {
//                nullFlags[i] = pageBuf[dataStart++];
//            }
            for (int i = 0; i < nullBytesCount; i++) {
                nullFlags[i] = pageBuf[dataStart++];
            }
            // 读数据
            Object[] row = new Object[columns.size()];
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
//                                row[colIdx++] = ByteUtil.toInt(ArrayUtils.subarray(pageBuf, dataStart, dataStart += 4));
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
                            row[colIdx++] = new java.sql.Date(ByteUtil.toLong(pageBuf, dataStart));
                            dataStart += 8;
                            break;
                        case TIMESTAMP:
                            row[colIdx++] = new java.sql.Timestamp(ByteUtil.toLong(pageBuf, dataStart));
                            dataStart += 8;
                            break;
                        case TIME:
                            row[colIdx++] = new java.sql.Time(ByteUtil.toLong(pageBuf, dataStart));
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
                                byte[] bytes = persistence.readExtraData(tablePath, extraId);
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
            data.add(row);
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

    private void alterColumn(Column column, int deleteIndex) {
        DataPage pataPage = persistence.readPage(tablePath);
//        DataExtent oldDataExtend = persistence.readExtent(tablePath);
        if (pataPage == null) return; // 没有数据，直接返回
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
        DataExtent oldDataExtend = persistence.readExtent(tmpTablePath);
        Map<String, Object> row = new LinkedHashMap<>();
        boolean isAdd = deleteIndex < 0;
        while (oldDataExtend != null) {
            byte[] byteBuf;
            int pageOffset = 0;
            while ((byteBuf = oldDataExtend.getPage(pageOffset++)) != null) {
                List<Object[]> data = transformToData(byteBuf, nullBytesCount);
                for (Object[] d : data) {
                    int i = 0;
                    Object[] pkValues = pkNames == null ? null : new Object[pkNames.length];
                    int pkIndex = 0;
                    for (String colName : colNameSet) {
                        if (deleteIndex == i) {
                            i++;
                            continue;
                        }
                        row.put(colName, d[i++]);
                        if (ArrayUtils.contains(pkNames, colName)) {
                            pkValues[pkIndex++] = row.get(colName);
                        }
                    }
                    if (isAdd) {
                        row.put(column.getName(), column.defaultValue());
                    }
                    DataPage dataPage = this.insertData(row);
                    // add index
                    if (pkValues != null) {
                        for (i = 0; i < pkNames.length; i++) {
                            Column col = table.getColumn(pkNames[i]);
                            tableIndex.insertIndex(col, valueToBytes(col, pkValues[i]), dataPage);
                        }
                    }
                }
            }
            oldDataExtend = persistence.readExtent(tablePath, oldDataExtend.getFileId(), oldDataExtend.getOffset() + 1);
        }
        persistence.delete(tmpTablePath);
    }

    private DiskTable diskTable() {
        return (DiskTable) table;
    }
}
