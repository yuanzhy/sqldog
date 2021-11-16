package com.yuanzhy.sqldog.server.memory;

import com.google.common.collect.Sets;
import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.core.Constraint;
import com.yuanzhy.sqldog.server.core.DML;
import com.yuanzhy.sqldog.server.core.Serial;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.core.constant.ConstraintType;
import com.yuanzhy.sqldog.server.core.constant.DataType;
import com.yuanzhy.sqldog.core.util.Asserts;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public class TableMemoryImpl extends MemoryBase implements Table, DML {
    /** 列 */
    private final Map<String, Column> columnMap;
    /** 约束 */
    private final Constraint primaryKey;
    private final Serial serial;
    private final Set<Constraint> constraint;

    /**
     * 数据
     * key = pk value
     * value = row data
     *    rowMap: key = colName
     *             value = row
     */
    private final Map<Object, Map<String, Object>> data = new HashMap<>();

    /**
     * 唯一索引
     * key = unique column value
     * value = pk value
     */
    private final Map<String, Object> uniqueMap = new HashMap<>();

    TableMemoryImpl(String name, Map<String, Column> columnMap, Constraint primaryKey, Set<Constraint> constraint, Serial serial) {
        super(name.toUpperCase());
        this.columnMap = columnMap;
        this.primaryKey = primaryKey;
        this.constraint = constraint;
        this.serial = serial;
    }
    /*
                              Table "public.company"
         Column  |     Type      | Collation | Nullable | Default
        ---------+---------------+-----------+----------+---------
         id      | integer       |           | not null |
         name    | text          |           | not null |
         age     | integer       |           | not null |
         address | character(50) |           |          |
         salary  | real          |           |          |
        Indexes:
            "company_pkey" PRIMARY KEY, btree (id)
     */
    @Override
    public String toPrettyString() {
        return "\t Table \"" + name + "\"\n" +
                joinByVLine("Column", "Type", "Nullable", "Default", "Description") + "\n" +
                genHLine(5) + "\n" +
                columnMap.values().stream().map(Column::toPrettyString).collect(Collectors.joining("\n")) + "\n" +
                "Constraint:\n" +
                "    " + primaryKey.toPrettyString() + "\n" +
                constraint.stream().map(Constraint::toPrettyString).map(s -> "    " + s).collect(Collectors.joining("\n"))
                ;
    }

    @Override
    public void drop() {
        this.columnMap.clear();
        this.constraint.clear();
        this.uniqueMap.clear();
        this.data.clear();
    }

    private String getPkName() {
        return primaryKey.getColumnNames()[0]; // TODO 暂不支持联合主键
    }

    @Override
    public Object insert(Map<String, Object> values) {
        // check
        this.checkData(values);
        // generate pk
        Object pkValue;
        String pkName = getPkName();
        if (values.containsKey(pkName)) {
            pkValue = values.get(pkName);
            // check pk
            Asserts.isFalse(data.containsKey(pkValue), "Primary key conflict：" + pkValue);
        } else if (columnMap.get(pkName).getDataType().isSerial()) {
            pkValue = this.serial.next();
            values.put(pkName, pkValue);
        } else {
            throw new IllegalArgumentException("Primary key must be not null");
        }
        // check constraint
        this.checkConstraint(values);
        // add data
        synchronized (data) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (Map.Entry<String, Column> entry : columnMap.entrySet()) {
                String columnName = entry.getKey();
                Column column = entry.getValue();
                //不包含，此列，则赋予defaultValue
                if (!values.containsKey(columnName)) {
                    row.put(columnName, column.defaultValue());
                    continue;
                }
                Object value = values.get(columnName);
//                if (value == null) {
//                    row.put(columnName, null);
//                    continue;
//                }
//                value = column.getDataType().parseRawValue(value.toString());
                row.put(columnName, value);
            }
            for (Constraint c : this.constraint) {
                String[] columnNames = c.getColumnNames();
                if (c.getType() == ConstraintType.UNIQUE) {
                    String uniColValue = Arrays.stream(columnNames).map(cn -> values.get(cn).toString()).collect(Collectors.joining("_"));
                    uniqueMap.put(uniColValue, pkValue);
                }
            }
            this.data.put(pkValue, row);
        }
        return pkValue;
    }

    private void checkData(Map<String, Object> values) {
        for (Column column : columnMap.values()) {
            Object value = values.get(column.getName());
            if (!column.isNullable() && value == null && column.defaultValue() == null) {
                throw new IllegalArgumentException("'" + column.getName() + "' is not null");
            }
            if (value == null) {
                continue;
            }
            Asserts.isTrue(column.getDataType().isAssignable(value), "DataType mismatch, " + column.getName() + ":" + value);
            if (column.getDataType().isHasLength()) {
                Asserts.isTrue(value.toString().length() <= column.getPrecision(), "Data length over range, " + column.getName() + "(" + column.getPrecision() + "): " + value);
            }
            if (column.getScale() > 0) {
                BigDecimal d = (BigDecimal) value;
                d = d.setScale(column.getScale(), BigDecimal.ROUND_HALF_UP);
                values.put(column.getName(), d);
            }
        }
    }

    /**
     * @param values
     */
    private void checkConstraint(Map<String, Object> values) {
        for (Constraint c : this.constraint) {
            String[] columnNames = c.getColumnNames();
            if (c.getType() == ConstraintType.UNIQUE) {
                String uniColValue = Arrays.stream(columnNames).map(cn -> values.get(cn).toString()).collect(Collectors.joining("_"));
                boolean contains = uniqueMap.containsKey(uniColValue);
                Asserts.isTrue(contains, "唯一约束冲突：" + c.getName());
            } else if (c.getType() == ConstraintType.FOREIGN_KEY) {
                //
            }
        }
    }

//    @Override
//    public int delete(Object id) {
//        Object removeValue = data.remove(id);
//        return removeValue == null ? 0 : 1;
//    }

//    @Override
//    public int update(Map<String, Object> updates, Object id) {
//        Map<String, Object> row = data.get(id);
//        if (row == null) {
//            return 0;
//        }
//        this.checkData(updates);
//        synchronized (row) {
//            for (String colName : columnMap.keySet()) {
//                if (updates.containsKey(colName)) {
//                    Object value = updates.get(colName);
//                    row.put(colName, value);
//                }
//            }
//            return 1;
//        }
//    }

    @Override
    public synchronized int deleteBy(SqlDelete sqlDelete) {
        int count = 0;
        SqlNode condition = sqlDelete.getCondition();
        if (condition == null) {
            count = data.size();
            this.truncate();
        } else if (condition instanceof SqlBasicCall) {
            Set<Map<String, Object>> dataList = handleWhere(data.values(), (SqlBasicCall)condition);
            String pkName = getPkName();
            for (Map<String, Object> row : dataList) {
                Map<String, Object> deletedRow = data.remove(row.get(pkName));
                if (deletedRow == null) {
                    continue;
                }
                count++;
                // 删除唯一索引
                for (Constraint c : this.constraint) {
                    String[] columnNames = c.getColumnNames();
                    if (c.getType() == ConstraintType.UNIQUE) {
                        String uniColValue = Arrays.stream(columnNames).map(cn -> row.get(cn).toString()).collect(Collectors.joining("_"));
                        uniqueMap.remove(uniColValue);
                    }
                }
            }
        } else {
            throw new UnsupportedOperationException("not supported: " + condition);
        }
        return count;
    }

    @Override
    public synchronized int updateBy(SqlUpdate sqlUpdate) {
        List<String> colList = sqlUpdate.getTargetColumnList().stream().map(SqlNode::toString).collect(Collectors.toList());
        Asserts.isFalse(colList.contains(getPkName()), "Temporary unsupported update primary key");
        List<Object> valList = new ArrayList<>(colList.size());
        for (int i = 0; i < colList.size(); i++) {
            DataType dt = columnMap.get(colList.get(i)).getDataType();
            SqlNode s = sqlUpdate.getSourceExpressionList().get(i);
            if (!(s instanceof SqlLiteral)) {
                throw new UnsupportedOperationException("not supported: " + s.toString());
            }
            valList.add(parseValue(s, dt));
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
            dataList = data.values();
        } else if (condition instanceof SqlBasicCall) {
            dataList = handleWhere(data.values(), (SqlBasicCall)condition);
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

    private Set<Map<String, Object>> handleWhere(Collection<Map<String, Object>> sources, SqlBasicCall condition) {
        SqlKind kind = condition.getOperator().getKind();
        SqlNode left = condition.getOperandList().get(0);
        SqlNode right = condition.getOperandList().size() > 1 ? condition.getOperandList().get(1): null;
        if (kind == SqlKind.AND) {
            Set<Map<String, Object>> leftData, rightData;
            if (left instanceof SqlBasicCall) {
                leftData = handleWhere(sources, (SqlBasicCall) left);
            } else {
                throw new UnsupportedOperationException("operation not support: " + left.toString());
            }
            if (right instanceof SqlBasicCall) {
                rightData = handleWhere(sources, (SqlBasicCall) right);
            } else {
                throw new UnsupportedOperationException("operation not support: " + right.toString());
            }
            return Sets.intersection(leftData, rightData);
        } else if (kind == SqlKind.OR) {
            Set<Map<String, Object>> leftData, rightData;
            if (left instanceof SqlBasicCall) {
                leftData = handleWhere(sources, (SqlBasicCall) left);
            } else {
                throw new UnsupportedOperationException("operation not support: " + left.toString());
            }
            if (right instanceof SqlBasicCall) {
                rightData = handleWhere(sources, (SqlBasicCall) right);
            } else {
                throw new UnsupportedOperationException("operation not support: " + right.toString());
            }
            return Sets.union(leftData, rightData);
        }
        if (left instanceof SqlBasicCall) {
            // where ID + AGE > 15 // 暂不支持
            throw new UnsupportedOperationException("operation not support: " + left.toString());
        }
        String leftString = left.toString();
        // where TT.ID < 15
        String colName = leftString.contains(".") ? StringUtils.substringAfter(leftString, ".") : leftString;
        Predicate<Map<String, Object>> fn = null;
        DataType dt = columnMap.get(colName).getDataType();
        Object val = parseValue(right, dt);
        if (kind == SqlKind.BETWEEN) {
            Object val2 = parseValue(condition.getOperandList().get(2), dt);
            fn = m -> m.get(colName) != null
                    && ObjectUtils.compare((Comparable)m.get(colName), (Comparable)val) >= 0
                    && ObjectUtils.compare((Comparable)m.get(colName), (Comparable)val2) <= 0;
        } else if (kind == SqlKind.EQUALS) {
            fn = m -> val != null && val.equals(m.get(colName));
        } else if (kind == SqlKind.NOT_EQUALS) {
            fn = m -> val != null && !val.equals(m.get(colName));
        } else if (kind == SqlKind.IN) {
            fn = m -> m.get(colName) != null && ((List)val).contains(m.get(colName));
        } else if (kind == SqlKind.NOT_IN) {
            fn = m -> m.get(colName) != null && !((List)val).contains(m.get(colName));
        } /*else if (kind == SqlKind.EXISTS) {

        } */else if (kind == SqlKind.IS_NULL) {
            fn = m -> m.get(colName) == null;
        } else if (kind == SqlKind.IS_NOT_NULL) {
            fn = m -> m.get(colName) != null;
        } else if (kind == SqlKind.LESS_THAN) {
            fn = m -> m.get(colName) != null && ObjectUtils.compare((Comparable)m.get(colName), (Comparable)val) < 0;
        } else if (kind == SqlKind.LESS_THAN_OR_EQUAL) {
            fn = m -> m.get(colName) != null && ObjectUtils.compare((Comparable)m.get(colName), (Comparable)val) <= 0;
        } else if (kind == SqlKind.GREATER_THAN) {
            fn = m -> m.get(colName) != null && ObjectUtils.compare((Comparable)m.get(colName), (Comparable)val) > 0;
        } else if (kind == SqlKind.GREATER_THAN_OR_EQUAL) {
            fn = m -> m.get(colName) != null && ObjectUtils.compare((Comparable)m.get(colName), (Comparable)val) >= 0;
        } else {
            throw new UnsupportedOperationException("operation not support: " + condition.toString());
        }
        // TODO + -
        return sources.stream().filter(fn).collect(Collectors.toSet());
    }

    private Object parseValue(SqlNode sqlNode, DataType dt) {
        if (sqlNode == null) {
            return null;
        }
        Object val;
        if (sqlNode instanceof SqlLiteral) {
            val = dt.parseValue(((SqlLiteral) sqlNode).toValue());
        } else if (sqlNode instanceof SqlNodeList) {
            val = ((SqlNodeList) sqlNode).getList().stream().map(s -> {
                if (s instanceof SqlLiteral) {
                    return dt.parseValue(((SqlLiteral) s).toValue());
                }
                throw new UnsupportedOperationException("operation not support: " + s.toString());
            }).collect(Collectors.toList());
        } else {
            throw new UnsupportedOperationException("operation not support: " + sqlNode.toString());
        }
        return val;
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

    @Override
    public Column getColumn(String name) {
        return this.columnMap.get(name);
    }

    @Override
    public Map<String, Column> getColumns() {
        return Collections.unmodifiableMap(this.columnMap);
    }

    @Override
    public List<Object[]> getData() {
        return data.values().stream().map(m -> m.values().toArray()).collect(Collectors.toList());
    }

    @Override
    public DML getDML() {
        return this;
    }

    @Override
    public synchronized void addColumn(Column column) {
        if (this.columnMap.containsKey(column.getName())) {
            throw new IllegalArgumentException(column.getName() + " exists");
        }
        this.columnMap.put(column.getName(), column);
        this.data.forEach((id, row) -> {
            row.put(column.getName(), column.defaultValue());
        });
    }

    @Override
    public synchronized void dropColumn(String columnName) {
        this.columnMap.remove(columnName);
        this.data.forEach((id, row) -> {
            row.remove(columnName);
        });
    }

    @Override
    public synchronized void truncate() {
        this.data.clear();
        this.uniqueMap.clear();
    }

//    @Override
//    public Query getQuery() {
//        return this.query;
//    }
}
