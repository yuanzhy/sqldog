package com.yuanzhy.sqldog.server.storage.disk;

import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.core.TableData;
import com.yuanzhy.sqldog.server.storage.base.AbstractTableData;
import com.yuanzhy.sqldog.server.util.ByteUtil;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 存储设计：
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/4
 */
public class DiskTableData extends AbstractTableData implements TableData {
    private static final long MAX_FILE_SIZE = 1024 * 1024 * 1024;
    private static final int PAGE_SIZE = 16 * 1024;
    private static final int PAGE_HEADER_SIZE = 16;
    private final String storagePath;
    DiskTableData(Table table, String storagePath) {
        super(table);
        this.storagePath = storagePath;
    }
    @Override
    public Object[] insert(Map<String, Object> values) {
        // check
        this.checkData(values);
        // generate pk
        Object[] pkValues = generatePkValues(values);
        // check pk TODO
//        String pkValue = Arrays.stream(pkValues).map(Object::toString).collect(Collectors.joining(UNITED_SEP));
//        Asserts.isFalse(pkSet.contains(pkValue), "Primary key conflict：" + Arrays.stream(pkValues).map(Object::toString).collect(Collectors.joining(", ")));
//        pkSet.add(pkValue);

        // check constraint
//        this.checkConstraint(values); TODO
//        for (Constraint c : table.getConstraints()) {
//            String[] columnNames = c.getColumnNames();
//            if (c.getType() == ConstraintType.UNIQUE) {
//                String uniqueKey = uniqueColName(columnNames);
//                String uniColValue = uniqueColValue(columnNames, values);
//                uniqueMap.computeIfAbsent(uniqueKey, k -> new HashSet<>()).add(uniColValue);
//            }
//        }
        // add data
        Map<String, Object> row = normalizeData(values);
        List<Byte> dataBytes = transformToBinary(row);
        byte[] pageBuf = new byte[PAGE_SIZE];

        File file = new File(storagePath, table.getName() + "_0001.tbd"); // 先写死一个名字
        if (file.exists()) {
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                long lastPageIdx = raf.length() - PAGE_SIZE;
                raf.seek(lastPageIdx); // 定位到最后一个page的起始位置
                int availableLen = raf.read(pageBuf); // 读取一个数据页
//                int freeLen = PAGE_SIZE - availableLen;
                short freeStart = ByteUtil.toShort(new byte[]{pageBuf[4], pageBuf[5]});
                short freeEnd = ByteUtil.toShort(new byte[]{pageBuf[6], pageBuf[7]});
                // 剩余空间不够存储本条记录
                if (freeEnd - freeStart < dataBytes.size()) {
                    // 单个文件大于1G, 新申请一个
                    if (raf.length() >= MAX_FILE_SIZE) {
                        // 实体文件过大，新开一个 TODO


                    } else {
                        fillPageBuf(dataBytes, pageBuf, (short)PAGE_HEADER_SIZE, (short)PAGE_SIZE);
                        // page回写文件
//                        lastPageIdx = raf.length();
                        raf.seek(raf.length());
                        raf.write(pageBuf);
                    }
                } else {
                    fillPageBuf(dataBytes, pageBuf, freeStart, freeEnd);
                    // page回写文件
                    raf.seek(lastPageIdx);
                    raf.write(pageBuf);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            // 填充 bytes 数据，刷入磁盘
            fillPageBuf(dataBytes, pageBuf, (short)PAGE_HEADER_SIZE, (short)PAGE_SIZE);
            try {
                FileUtils.writeByteArrayToFile(file, pageBuf);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return pkValues;
    }

    private void fillPageBuf(List<Byte> dataBytes, byte[] pageBuf, short freeStart, short freeEnd) {
        // Page Header
        //  - CHKSUM 未实现
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
        for (Byte dataByte : dataBytes) {
            pageBuf[freeStart++] = dataByte;
        }
        // 更新freeStart字段
        byte[] startBytes = ByteUtil.toBytes(freeStart);
        pageBuf[4] = startBytes[0];
        pageBuf[5] = startBytes[1];
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
            switch (column.getDataType()) {
                case INT:
                case SERIAL:
                    addAll(dataList, ByteUtil.toBytes(((Number) value).intValue()));
                    break;
                case BIGINT:
                case BIGSERIAL:
                    addAll(dataList, ByteUtil.toBytes(((Number) value).longValue()));
                    break;
                case SMALLINT:
                case SMALLSERIAL:
                    addAll(dataList, ByteUtil.toBytes(((Number) value).shortValue()));
                    break;
                case TINYINT:
                    dataList.add(((Number) value).byteValue());
                    break;
                case FLOAT:
                    addAll(dataList, ByteUtil.toBytes(((Number) value).floatValue()));
                    break;
                case DOUBLE:
                    addAll(dataList, ByteUtil.toBytes(((Number) value).doubleValue()));
                    break;
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
                    addAll(dataList, ByteUtil.toBytes(v));
                    break;
                case BOOLEAN:
                    byte b = (byte)(((Boolean)value) ? 0b1 : 0b0);
                    dataList.add(b);
                    break;
                case BYTEA:
                case TEXT:
                case ARRAY:
                case JSON:
                    throw new UnsupportedOperationException("暂未实现大字段存储");
                default: // VARCHAR, CHAR, DECIMAL, NUMERIC
                    byte[] strBytes;
                    try {
                        strBytes = value.toString().getBytes(StorageConst.CHARSET);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    short len = (short)strBytes.length;
                    addAll(dataList, ByteUtil.toBytes(len));
                    addAll(dataList, strBytes);
            }
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
    public int deleteBy(SqlDelete sqlDelete) {
        return 0;
    }

    @Override
    public int updateBy(SqlUpdate sqlUpdate) {
        return 0;
    }

    @Override
    public void truncate() {
//        persistence.delete(persistence.resolvePath(storagePath, StorageConst.TABLE_DATA_PATH));
//        persistence.delete(persistence.resolvePath(storagePath, StorageConst.TABLE_INDEX_PATH));
    }

    @Override
    @Deprecated
    public List<Object[]> getData() {
        List<Object[]> data = new ArrayList<>();
        File file = new File(storagePath, table.getName() + "_0001.tbd"); // 先写死一个名字
        byte[] pageBuf = new byte[PAGE_SIZE];
        Collection<Column> columns = table.getColumns().values();
        int nullableCount = (int)columns.stream().filter(Column::isNullable).count();
        int nullBytesCount = nullableCount % 8 > 0 ? nullableCount / 8 + 1 : nullableCount / 8;
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            int availableLen = raf.read(pageBuf); // 读取一个数据页
            int dataStart = PAGE_HEADER_SIZE;
            short freeStart = ByteUtil.toShort(new byte[]{pageBuf[4], pageBuf[5]});
            while (dataStart < freeStart) {  // 读取多行数据
                short dataHeader = ByteUtil.toShort(new byte[]{pageBuf[dataStart++], pageBuf[dataStart++]});
                boolean deleted = ((dataHeader >> 15) & 0b1) == 1;
                if (deleted) continue;
                int end = dataStart + (dataHeader & 0b0111111111111111);
                // 读取nullFlag
                byte[] nullFlags = new byte[nullBytesCount];
                for (int i = nullBytesCount - 1; i >= 0; i--) {
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
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return data;
    }

    @Override
    public void addColumn(Column column) {

    }

    @Override
    public void dropColumn(String columnName) {

    }
}
