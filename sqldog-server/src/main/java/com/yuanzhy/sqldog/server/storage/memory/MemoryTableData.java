package com.yuanzhy.sqldog.server.storage.memory;

import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.core.Constraint;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.core.TableData;
import com.yuanzhy.sqldog.server.core.constant.ConstraintType;
import com.yuanzhy.sqldog.server.storage.base.AbstractTableData;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUpdate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/4
 */
public class MemoryTableData extends AbstractTableData implements TableData {

    protected static final String UNITED_SEP = "#~^~#";

    /** 数据 */
    private final List<Map<String, Object>> data = new ArrayList<>();

    /**
     * pk values
     */
    private final Set<String> pkSet = new HashSet<>();

    /**
     * 唯一索引
     * key = columnName : 联合唯一则拼接字符串
     * value = uniqueValueSet 联合唯一则拼接列的value
     */
    private final Map<String, Set<String>> uniqueMap = new HashMap<>();

    MemoryTableData(Table table) {
        super(table);
    }

    @Override
    public synchronized Object[] insert(Map<String, Object> values) {
        // check
        this.checkData(values);
        // generate pk
        Object[] pkValues = generatePkValues(values);
        // check pk
        String pkValue = Arrays.stream(pkValues).map(Object::toString).collect(Collectors.joining(UNITED_SEP));
        Asserts.isFalse(pkSet.contains(pkValue), "Primary key conflict：" + Arrays.stream(pkValues).map(Object::toString).collect(Collectors.joining(", ")));
        pkSet.add(pkValue);
        // check constraint
        this.checkConstraint(values);
        for (Constraint c : table.getConstraints()) {
            String[] columnNames = c.getColumnNames();
            if (c.getType() == ConstraintType.UNIQUE) {
                String uniqueKey = uniqueColName(columnNames);
                String uniColValue = uniqueColValue(columnNames, values);
                uniqueMap.computeIfAbsent(uniqueKey, k -> new HashSet<>()).add(uniColValue);
            }
        }
        // add data
        this.data.add(normalizeData(values));
        return pkValues;
    }

    @Override
    public synchronized int deleteBy(SqlDelete sqlDelete) {
        int count = 0;
        SqlNode condition = sqlDelete.getCondition();
        if (condition == null) {
            count = data.size();
            this.truncate();
        } else if (condition instanceof SqlBasicCall) {
            Set<Map<String, Object>> dataList = handleWhere(data, (SqlBasicCall)condition);
            count = dataList.size();
            String[] pkNames = table.getPkColumnName();
            for (Map<String, Object> row : dataList) {
                // 删除主键索引
                if (pkNames != null) {
                    pkSet.remove(uniqueColValue(pkNames, row));
                }
                // 删除唯一索引
                for (Constraint c : table.getConstraints()) {
                    String[] columnNames = c.getColumnNames();
                    if (c.getType() == ConstraintType.UNIQUE) {
                        uniqueMap.getOrDefault(uniqueColName(columnNames), Collections.emptySet()).remove(uniqueColValue(columnNames, row));
                    }
                }
            }
            data.removeAll(dataList);
        } else {
            throw new UnsupportedOperationException("not supported: " + condition);
        }
        return count;
    }

    @Override
    public synchronized int updateBy(SqlUpdate sqlUpdate) {
        List<String> colList = sqlUpdate.getTargetColumnList().stream().map(SqlNode::toString).collect(Collectors.toList());
        if (table.getPrimaryKey() != null) {
            for (String columnName : table.getPrimaryKey().getColumnNames()) {
                Asserts.isFalse(colList.contains(columnName), "Temporary unsupported update primary key");
            }
        }
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
        // checkConstraint TODO
//        for (Constraint c : constraint) {
//            String[] columnNames = c.getColumnNames();
//            if (c.getType() == ConstraintType.UNIQUE) {
//                String uniColValue = Arrays.stream(columnNames).map(cn -> values.get(cn).toString()).collect(Collectors.joining("_"));
//                boolean contains = uniqueMap.containsKey(uniColValue);
//                Asserts.isTrue(contains, "Unique key conflict：" + c.getName());
//            } else if (c.getType() == ConstraintType.FOREIGN_KEY) {
//                //
//            }
//        }
        SqlNode condition = sqlUpdate.getCondition();
        Collection<Map<String, Object>> dataList;
        if (condition == null) {
            dataList = data;
        } else if (condition instanceof SqlBasicCall) {
            dataList = handleWhere(data, (SqlBasicCall)condition);
        } else {
            throw new UnsupportedOperationException("not supported: " + condition);
        }
        for (Map<String, Object> row : dataList) {
            for (int i = 0; i < colList.size(); i++) {
                row.put(colList.get(i), valList.get(i));
            }
        }
        return dataList.size();
    }

    @Override
    public void truncate() {
        this.data.clear();
    }

    @Override
    public List<Object[]> getData() {
        return data.stream().map(m -> m.values().toArray()).collect(Collectors.toList());
    }

    @Override
    public void addColumn(Column column) {
        this.data.forEach(row -> row.put(column.getName(), column.defaultValue()));
    }

    @Override
    public void dropColumn(String columnName) {
        this.data.forEach(row -> row.remove(columnName));
    }

    /**
     * @param values
     */
    private void checkConstraint(Map<String, Object> values) {
        for (Constraint c : table.getConstraints()) {
            String[] columnNames = c.getColumnNames();
            if (c.getType() == ConstraintType.UNIQUE) {
                boolean contains = uniqueMap.getOrDefault(uniqueColName(columnNames), Collections.emptySet()).contains(uniqueColValue(columnNames, values));
                Asserts.isTrue(contains, "唯一约束冲突：" + c.getName());
            } else if (c.getType() == ConstraintType.FOREIGN_KEY) {
                // TODO 外键约束
            }
        }
    }

    private String uniqueColName(String[] columnNames) {
        if (columnNames.length == 1) {
            return columnNames[0];
        }
        return Arrays.stream(columnNames).collect(Collectors.joining(UNITED_SEP));
    }

    private String uniqueColValue(String[] columnNames, Map<String, Object> dataRow) {
        return Arrays.stream(columnNames).map(cn -> String.valueOf(dataRow.get(cn))).collect(Collectors.joining(UNITED_SEP));
    }


    //    private boolean isMatch(Map<String, Object> row, Map<String, Object> wheres) {
//        for (Map.Entry<String, Object> whereEntry : wheres.entrySet()) {
//            Object whereValue = whereEntry.getValue();
//            Object whereKey = whereEntry.getKey();
//            if (!Objects.equals(whereValue, row.get(whereKey))) {
//                return false;
//            }
//        }
//        return true;
//    }
}
