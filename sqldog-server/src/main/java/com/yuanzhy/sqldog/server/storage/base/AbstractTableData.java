package com.yuanzhy.sqldog.server.storage.base;

import com.google.common.collect.Sets;
import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.core.TableData;
import com.yuanzhy.sqldog.server.core.constant.DataType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/4/4
 */
public abstract class AbstractTableData implements TableData {

    protected final Table table;
    protected AbstractTableData(Table table) {
        this.table = table;
    }

    protected void checkData(Map<String, Object> values) {
        for (Column column : table.getColumns().values()) {
            Object value = values.get(column.getName());
            if (!column.isNullable() && value == null && column.defaultValue() == null) {
                throw new IllegalArgumentException("'" + column.getName() + "' is not null");
            }
            value = checkVal(column, value);
            values.put(column.getName(), value);
        }
    }

    protected Object checkVal(Column column, Object val) {
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

    /**
     * 规范化数据
     *   顺序统一，赋默认值，char补空格等
     * @param values 未经处理的数据
     * @return
     */
    protected Map<String, Object> normalizeData(Map<String, Object> values) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (Map.Entry<String, Column> entry : table.getColumns().entrySet()) {
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
            if (value != null && column.getDataType() == DataType.CHAR && value.toString().length() < column.getPrecision()) {
                value = StringUtils.rightPad(value.toString(), column.getPrecision());
            }
            row.put(columnName, value);
        }
        return row;
    }

    protected Object[] generatePkValues(Map<String, Object> values) {
        if (table.getPrimaryKey() == null) {
            return null;
        }
        String[] pkNames = table.getPrimaryKey().getColumnNames();
        Object[] pkValues = new Object[pkNames.length];
        for (int i = 0; i < pkNames.length; i++) {
            String pkName = pkNames[i];
            Object onePkValue;
            if (values.containsKey(pkName)) {
                onePkValue = values.get(pkName);
            } else if (table.getColumn(pkName).getDataType().isSerial()) {
                onePkValue = table.getSerial().next();
                values.put(pkName, onePkValue);
            } else {
                throw new IllegalArgumentException("Primary key must be not null");
            }
            pkValues[i] = onePkValue;
        }
        return pkValues;
    }

    protected Set<Map<String, Object>> handleWhere(Collection<Map<String, Object>> sources, SqlBasicCall condition) {
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
        DataType dt = table.getColumn(colName).getDataType();
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

    protected Object parseValue(SqlNode sqlNode, DataType dt) {
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
}
