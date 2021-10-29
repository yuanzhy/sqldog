package com.yuanzhy.sqldog.memory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;

import com.yuanzhy.sqldog.core.Column;
import com.yuanzhy.sqldog.core.Constraint;
import com.yuanzhy.sqldog.core.DML;
import com.yuanzhy.sqldog.core.Query;
import com.yuanzhy.sqldog.core.Serial;
import com.yuanzhy.sqldog.core.Table;
import com.yuanzhy.sqldog.core.constant.ConstraintType;
import com.yuanzhy.sqldog.util.Asserts;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public class TableMemoryImpl implements Table, DML {
    /** 名称 */
    private final String name;
    /** 列 */
    private final Map<String, Column> columnMap;
    /** 约束 */
    private final Constraint primaryKey;
    private final Serial serial;
    private final Set<Constraint> constraint;

    /** 数据 */
    private final Map<Object, Map<String, Object>> data = new ConcurrentHashMap<>();

    /** 索引 */
//    private final List<>

    private final Query query = new QueryMemoryImpl();

    TableMemoryImpl(String name, Map<String, Column> columnMap, Constraint primaryKey, Set<Constraint> constraint, Serial serial) {
        this.name = name;
        this.columnMap = columnMap;
        this.primaryKey = primaryKey;
        this.constraint = constraint;
        this.serial = serial;
    }

    @Override
    public String getName() {
        return this.name;
    }

    private String getPkName() {
        return primaryKey.getColumnNames()[0]; // TODO 暂不支持联合主键
    }

    @Override
    public Object insert(Map<String, Object> values) {
        // check nullable
        for (Column column : columnMap.values()) {
            if (column.isNullable()) {
                if (!values.containsKey(column.getName())) {
                    values.put(column.getName(), null); // 放置空值
                }
            } else if (values.get(column.getName()) == null) {
                throw new IllegalArgumentException("'" + column.getName() + "' is not null");
            }

        }
        // generate pk
        Object pkValue;
        String pkName = getPkName();
        if (values.containsKey(pkName)) {
            pkValue = values.get(pkName);
            // check pk
            Asserts.isTrue(data.containsKey(pkName), "主键值冲突：" + pkValue);
        } else if (columnMap.get(pkName).getDataType().isSerial()) {
            pkValue = this.serial.next();
            values.put(pkName, pkValue);
        } else {
            throw new IllegalArgumentException("主键值不能为空");
        }
        // check constraint
        this.checkConstraint(values);

        this.data.put(pkValue, values);
        return pkValue;
    }

    /**
     * TODO 唯一约束性能优化（索引）
     * @param values
     */
    private void checkConstraint(Map<String, Object> values) {
        for (Constraint c : this.constraint) {
            String[] columnNames = c.getColumnNames();
            if (c.getType() == ConstraintType.UNIQUE) {
                boolean contains = data.values().stream().anyMatch(row -> {
                    for (String columnName : columnNames) {
                        if (!Objects.equals(row.get(columnName), values.get(columnName))) {
                            return false;
                        }
                    }
                    return true;
                });
                Asserts.isTrue(contains, "唯一约束冲突：" + c.getName());
            } else if (c.getType() == ConstraintType.FOREIGN_KEY) {
                //
            }
        }
    }

    @Override
    public int delete(Object id) {
        Object removeValue = data.remove(id);
        return removeValue == null ? 0 : 1;
    }

    @Override
    public int update(Map<String, Object> updates, Object id) {
        Map<String, Object> row = data.get(id);
        if (row == null) {
            return 0;
        }
        row.putAll(updates);
        return 1;
    }

    @Override
    public synchronized int deleteBy(Map<String, Object> wheres) {
        Iterator<Map.Entry<Object, Map<String, Object>>> ite = data.entrySet().iterator();
        int count = 0;
        while (ite.hasNext()) {
            Map.Entry<Object, Map<String, Object>> entry = ite.next();
            Map<String, Object> row = entry.getValue();
            if (isMatch(row, wheres)) {
                ite.remove();
                count++;
            }
        }
        return count;
    }

    @Override
    public synchronized int updateBy(Map<String, Object> updates, Map<String, Object> wheres) {
        Asserts.isTrue(updates.containsKey(getPkName()), "暂不支持更新主键");
        this.checkConstraint(updates);// TODO update的唯一约束检查有bug，可能只更新了一行并且唯一键更新的和原有的一致
        int count = 0;
        for (Map.Entry<Object, Map<String, Object>> rowEntry : data.entrySet()) {
            Map<String, Object> row = rowEntry.getValue();
            if (isMatch(row, wheres)) {
                row.putAll(updates);
                count++;
            }
        }
        return count;
    }

    private boolean isMatch(Map<String, Object> row, Map<String, Object> wheres) {
        for (Map.Entry<String, Object> whereEntry : wheres.entrySet()) {
            Object whereValue = whereEntry.getValue();
            Object whereKey = whereEntry.getKey();
            if (!Objects.equals(whereValue, row.get(whereKey))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Map<String, Column> getColumn() {
        return Collections.unmodifiableMap(this.columnMap);
    }

    @Override
    public DML getDML() {
        return this;
    }

    @Override
    public Query getQuery() {
        return this.query;
    }

    private class QueryMemoryImpl implements Query {

        @Override
        public Map<String, Object> select(Object id) {
            Map<String, Object> r = data.get(id);
            return r == null ? null : Collections.unmodifiableMap(r);
        }

        @Override
        public List<Map<String, Object>> selectBy(SqlNode sqlNode) {
            handleSQL(sqlNode);
            return null;
        }

        private void handleSQL(SqlNode sqlNode) {
            SqlKind kind = sqlNode.getKind();
            switch (kind) {
                case SELECT:
                    handleSelect(sqlNode);
                    break;
                case UNION:
                    ((SqlBasicCall) sqlNode).getOperandList().forEach(node -> {
                        handleSQL(node);
                    });
                    break;
                case ORDER_BY:
                    handleOrderBy(sqlNode);
                    break;
            }
        }

        private void handleOrderBy(SqlNode node) {
            SqlOrderBy sqlOrderBy = (SqlOrderBy) node;
            SqlNode query = sqlOrderBy.query;
            handleSQL(query);
            SqlNodeList orderList = sqlOrderBy.orderList;
            handlerField(orderList);

            SqlNode fetch = sqlOrderBy.fetch;
            SqlNode offset = sqlOrderBy.offset;
            // TODO limit offset
        }


        private void handleSelect(SqlNode select) {
            SqlSelect sqlSelect = (SqlSelect) select;
            //TODO 改写SELECT的字段信息
            SqlNodeList selectList = sqlSelect.getSelectList();
            //字段信息
            selectList.getList().forEach(list -> {
                handlerField(list);
            });

            handlerFrom(sqlSelect.getFrom());

            if (sqlSelect.hasWhere()) {
                handlerField(sqlSelect.getWhere());
            }

            if (sqlSelect.hasOrderBy()) {
                handlerField(sqlSelect.getOrderList());
            }

            SqlNodeList group = sqlSelect.getGroup();
            if (group != null) {
                group.forEach(groupField -> {
                    handlerField(groupField);
                });
            }


            SqlNode fetch = sqlSelect.getFetch();
            if (fetch != null) {
                //TODO limit
            }

        }

        private void handlerFrom(SqlNode from) {
            SqlKind kind = from.getKind();

            switch (kind) {
                case IDENTIFIER:
                    //最终的表名
                    SqlIdentifier sqlIdentifier = (SqlIdentifier) from;
                    //TODO 表名的替换，所以在此之前就需要获取到模型的信息
                    System.out.println("==tablename===" + sqlIdentifier.toString());
                    break;
                case AS:
                    SqlBasicCall sqlBasicCall = (SqlBasicCall) from;
                    SqlNode selectNode = sqlBasicCall.getOperandList().get(0);
                    handleSQL(selectNode);
                    break;
                case JOIN:
                    SqlJoin sqlJoin = (SqlJoin) from;
                    SqlNode left = sqlJoin.getLeft();
                    handleSQL(left);
                    SqlNode right = sqlJoin.getRight();
                    handleSQL(right);
                    SqlNode condition = sqlJoin.getCondition();
                    handlerField(condition);
                    break;
                case SELECT:
                    handleSQL(from);
                    break;
            }
        }

        private void handlerField(SqlNode field) {
            SqlKind kind = field.getKind();
            switch (kind) {
                case AS:
                    SqlNode[] operands_as = ((SqlBasicCall) field).operands;
                    SqlNode left_as = operands_as[0];
                    handlerField(left_as);
                    break;
                case IDENTIFIER:
                    //表示当前为子节点
                    SqlIdentifier sqlIdentifier = (SqlIdentifier) field;
                    System.out.println("===field===" + sqlIdentifier.toString());
                    break;
                default:
                    if (field instanceof SqlBasicCall) {
                        SqlNode[] nodes = ((SqlBasicCall) field).operands;
                        for (int i = 0; i < nodes.length; i++) {
                            handlerField(nodes[i]);
                        }
                    }
                    if (field instanceof SqlNodeList) {
                        ((SqlNodeList) field).getList().forEach(node -> {
                            handlerField(node);
                        });
                    }
                    break;
            }
        }
    }

    public class MemoryTableBuilder {
        private String name;
        private Constraint primaryKey;
        private final Map<String, Column> columnMap = new LinkedHashMap<>();
        private final Set<Constraint> constraint = new HashSet<>();
        private Serial serial;
        public MemoryTableBuilder name(String name) {
            this.name = name;
            return this;
        }
        public MemoryTableBuilder addColumn(Column column) {
            this.columnMap.put(column.getName(), column);
            return this;
        }
        public MemoryTableBuilder addConstraint(Constraint constraint) {
            if (constraint.getType() == ConstraintType.PRIMARY_KEY) {
                this.primaryKey = constraint;
            } else {
                this.constraint.add(constraint);
            }
            return this;
        }

        public MemoryTableBuilder serial(long initValue, int step) {
            this.serial = new SerialMemoryImpl(0, 1);
            return this;
        }

        public Table build() {
            Asserts.hasText(name, "表名称不能为空");
            Asserts.notNull(primaryKey, "主键不能为空");
            Asserts.hasEle(columnMap, "列不能为空");
            Column pkColumn = columnMap.get(primaryKey.getColumnNames()[0]);
            Asserts.notNull(pkColumn, "主键列不存在");
            // setSerial
            if (this.serial == null && pkColumn.getDataType().isSerial()) {
                this.serial = new SerialMemoryImpl(0, 1); // 默认步长
            }
            return new TableMemoryImpl(name, columnMap, primaryKey, constraint, serial);
        }
    }
}
