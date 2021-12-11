package com.yuanzhy.sqldog.server.memory;

import com.google.common.collect.Sets;
import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.core.Constraint;
import com.yuanzhy.sqldog.server.core.DML;
import com.yuanzhy.sqldog.server.core.Serial;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.core.constant.ConstraintType;
import com.yuanzhy.sqldog.server.core.constant.DataType;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public class TableMemoryImpl extends MemoryBase implements Table, DML {

    private static final String UNITED_SEP = "#~^~#";
    /** 列 */
    private final Map<String, Column> columnMap;
    /** 约束 */
    private final Constraint primaryKey;
    private final Serial serial;
    private final Set<Constraint> constraint;

    /**
     * 数据
     */
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

    TableMemoryImpl(String name, Map<String, Column> columnMap, Constraint primaryKey, Set<Constraint> constraint, Serial serial) {
        super(name.toUpperCase());
        this.columnMap = columnMap;
        this.primaryKey = primaryKey;
        this.constraint = constraint;
        this.serial = serial;
    }

    @Override
    public void drop() {
        this.columnMap.clear();
        this.constraint.clear();
        this.uniqueMap.clear();
        this.data.clear();
    }

    @Override
    public String[] getPkColumnName() {
        return primaryKey == null ? null : primaryKey.getColumnNames();
    }

    @Override
    public Constraint getPrimaryKey() {
        return primaryKey;
    }

    @Override
    public List<Constraint> getConstraints() {
        List<Constraint> r = new ArrayList<>();
        if (primaryKey != null) {
            r.add(primaryKey);
        }
        if (constraint != null) {
            r.addAll(constraint);
        }
        return r.isEmpty() ? null : r;
    }

    @Override
    public synchronized Object[] insert(Map<String, Object> values) {
        // check
        this.checkData(values);
        // generate pk
        Object[] pkValues = null;
        if (primaryKey != null) {
            String[] pkNames = primaryKey.getColumnNames();
            pkValues = new Object[pkNames.length];
            for (int i = 0; i < pkNames.length; i++) {
                String pkName = pkNames[i];
                Object onePkValue;
                if (values.containsKey(pkName)) {
                    onePkValue = values.get(pkName);
                } else if (columnMap.get(pkName).getDataType().isSerial()) {
                    onePkValue = this.serial.next();
                    values.put(pkName, onePkValue);
                } else {
                    throw new IllegalArgumentException("Primary key must be not null");
                }
                pkValues[i] = onePkValue;
            }
            String pkValue = Arrays.stream(pkValues).map(Object::toString).collect(Collectors.joining(UNITED_SEP));
            // check pk
            Asserts.isFalse(pkSet.contains(pkValue), "Primary key conflict：" + Arrays.stream(pkValues).map(Object::toString).collect(Collectors.joining(", ")));
            pkSet.add(pkValue);
        }
        // check constraint
        this.checkConstraint(values);
        // add data
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
            if (column.getDataType() == DataType.CHAR && Objects.toString(value).length() < column.getPrecision()) {
                value = StringUtils.rightPad(Objects.toString(value), column.getPrecision());
            }
            row.put(columnName, value);
        }
        for (Constraint c : this.constraint) {
            String[] columnNames = c.getColumnNames();
            if (c.getType() == ConstraintType.UNIQUE) {
                String uniqueKey = uniqueColName(columnNames);
                String uniColValue = uniqueColValue(columnNames, values);
                uniqueMap.computeIfAbsent(uniqueKey, k -> new HashSet<>()).add(uniColValue);
            }
        }
        this.data.add(row);
        return pkValues;
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

    private void checkData(Map<String, Object> values) {
        for (Column column : columnMap.values()) {
            Object value = values.get(column.getName());
            if (!column.isNullable() && value == null && column.defaultValue() == null) {
                throw new IllegalArgumentException("'" + column.getName() + "' is not null");
            }
            value = checkVal(column, value);
            values.put(column.getName(), value);
        }
    }

    /**
     * @param values
     */
    private void checkConstraint(Map<String, Object> values) {
        for (Constraint c : this.constraint) {
            String[] columnNames = c.getColumnNames();
            if (c.getType() == ConstraintType.UNIQUE) {
                boolean contains = uniqueMap.getOrDefault(uniqueColName(columnNames), Collections.emptySet()).contains(uniqueColValue(columnNames, values));
                Asserts.isTrue(contains, "唯一约束冲突：" + c.getName());
            } else if (c.getType() == ConstraintType.FOREIGN_KEY) {
                //
            }
        }
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
            String[] pkNames = getPkColumnName();
            for (Map<String, Object> row : dataList) {
                // 删除主键索引
                if (pkNames != null) {
                    pkSet.remove(uniqueColValue(pkNames, row));
                }
                // 删除唯一索引
                for (Constraint c : this.constraint) {
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
        if (primaryKey != null) {
            for (String columnName : primaryKey.getColumnNames()) {
                Asserts.isFalse(colList.contains(columnName), "Temporary unsupported update primary key");
            }
        }
        List<Object> valList = new ArrayList<>(colList.size());
        for (int i = 0; i < colList.size(); i++) {
            Column column = columnMap.get(colList.get(i));
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

    private Object checkVal(Column column, Object val) {
        if (val != null) {
            //Asserts.isTrue(column.getDataType().getClazz().isInstance(value), "DataType mismatch, " + column.getName() + ":" + value);
            if (column.getDataType().isHasLength()) {
                Asserts.isTrue(val.toString().length() <= column.getPrecision(), "Data length over range, " + column.getName() + "(" + column.getPrecision() + "): " + val);
            }
            if (column.getScale() > 0) {
                BigDecimal d = (BigDecimal) val;
                d = d.setScale(column.getScale(), BigDecimal.ROUND_HALF_UP);
                val = d;
            }
        }
        return val;
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
        } else if (kind == SqlKind.LIKE) {
            if (val == null) {
                fn = m -> false;
            } else {
                Pattern pattern = Pattern.compile("^" + val.toString().replace("%", ".*").replace("_", ".") + "$");
                fn = m -> {
                    Object v = m.get(colName);
                    if (v == null) {
                        return false;
                    }
                    return pattern.matcher(v.toString()).matches();
                };
            }
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
        return data.stream().map(m -> m.values().toArray()).collect(Collectors.toList());
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
        this.data.forEach(row -> row.put(column.getName(), column.defaultValue()));
    }

    @Override
    public synchronized void dropColumn(String columnName) {
        this.columnMap.remove(columnName);
        this.data.forEach(row -> row.remove(columnName));
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
