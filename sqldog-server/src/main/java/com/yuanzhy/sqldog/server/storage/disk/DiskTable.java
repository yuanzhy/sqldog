package com.yuanzhy.sqldog.server.storage.disk;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.yuanzhy.sqldog.server.core.Base;
import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.core.Constraint;
import com.yuanzhy.sqldog.server.core.Persistable;
import com.yuanzhy.sqldog.server.core.Persistence;
import com.yuanzhy.sqldog.server.core.Serial;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.core.constant.ConstraintType;
import com.yuanzhy.sqldog.server.core.constant.DataType;
import com.yuanzhy.sqldog.server.storage.builder.ColumnBuilder;
import com.yuanzhy.sqldog.server.storage.builder.ConstraintBuilder;
import com.yuanzhy.sqldog.server.storage.memory.MemoryTable;
import com.yuanzhy.sqldog.server.storage.persistence.PersistenceFactory;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/3/29
 */
public class DiskTable extends MemoryTable implements Table, Persistable {
    private final String storagePath;
    private final Persistence persistence;

    private Map<String, Column> oldColumns = null;

    protected DiskTable(Base parent, String tablePath) {
        super(parent);
        this.persistence = PersistenceFactory.get();
        // 从硬盘meta文件中恢复table
        Map<String, Object> map = persistence.readMeta(tablePath);
        super.rename((String)map.get("name"));
        super.setDescription((String)map.get("description"));
        this.storagePath = persistence.resolvePath(this);
        super.tableData = new DiskTableData(this, storagePath);
        List<Map<String, Object>> columns = (List<Map<String, Object>>) map.get("columns");
        for (Map<String, Object> c : columns) {
            Column column = new ColumnBuilder().name((String)c.get("name")).dataType(DataType.of((String)c.get("dataType")))
                    .precision((int)c.get("precision")).scale((int)c.get("scale")).nullable((boolean)c.get("nullable"))
                    .defaultValue(map.get("defaultValue")).build();
            super.columnMap.put(column.getName(), column);
        }
        List<Map<String, Object>> constraints = (List<Map<String, Object>>) map.get("constraints");
        for (Map<String, Object> c : constraints) {
            ConstraintBuilder cb = new ConstraintBuilder().name((String)c.get("name")).type(ConstraintType.valueOf((String)c.get("type")));
            List<String> columnNames = (List<String>) c.get("columnNames");
            for (String columnName : columnNames) {
                cb.addColumnName(columnName);
            }
            Constraint constraint = cb.build();
            if (constraint.getType() == ConstraintType.PRIMARY_KEY) {
                this.primaryKey = constraint;
            } else {
                this.constraint.add(constraint);
            }
        }
    }

    public DiskTable(Base parent, String name, Map<String, Column> columnMap, Constraint primaryKey, Set<Constraint> constraint, Serial serial) {
        super(parent, name, columnMap, primaryKey, constraint, serial);
        this.persistence = PersistenceFactory.get();
        this.storagePath = persistence.resolvePath(this);
//        this.persistence();
        super.tableData = new DiskTableData(this, storagePath);
    }

    @Override
    protected void initTableData() {
//        super.tableData = new DiskTableData(this);
    }

    @Override
    public void setDescription(String description) {
        super.setDescription(description);
        this.persistence();
    }

    @Override
    public void addColumn(Column column) {
        // adding
        this.oldColumns = new LinkedHashMap<>(columnMap);
        try {
            super.addColumn(column);
            this.persistence();
        } finally {
            this.oldColumns = null;
        }
    }

    @Override
    public void dropColumn(String columnName) {
        super.dropColumn(columnName);
        this.persistence();
    }

    @Override
    public void updateColumnDescription(String colName, String description) {
        super.updateColumnDescription(colName, description);
        this.persistence();
    }

    //    @Override
//    public void truncate() {
//        super.truncate();
//        persistence.delete(persistence.resolvePath(storagePath, StorageConst.TABLE_DATA_PATH));
//        persistence.delete(persistence.resolvePath(storagePath, StorageConst.TABLE_INDEX_PATH));
//    }

    @Override
    public void drop() {
        super.drop();
        persistence.delete(storagePath);
    }

    public Map<String, Column> getOldColumns() {
        if (this.oldColumns == null) {
            return super.getColumns();
        } else {
            return this.oldColumns;
        }
    }

    @Override
    public void rename(String newName) {
        super.rename(newName);
        this.persistence();
        // TODO meta里的name属性改过了，目录的名称要不要改？
    }

    /*
     * {
     *   name: "",
     *   description: "",
     *   columns: [
     *     column: {
     *       name: "",
     *       description: "",
     *       dataType: "",
     *       precision: "",
     *       scale: "",
     *       nullable: "",
     *       defaultValue: "",
     *     }
     *   ],
     *   constraints: [
     *     constraint: {
     *       name: "",
     *       description: "",
     *       type: "",
     *       columnNames: ["", ""]
     *     }
     *   ],
     *   indexes: [
     *     // TODO
     *   ]
     * }
     */
    @Override
    public void persistence() {
        // columns
        JSONArray columnsJson = new JSONArray();
        for (Column c : getColumns().values()) {
            JSONObject columnJson = new JSONObject();
            columnJson.fluentPut("name", c.getName()).fluentPut("description", c.getDescription())
                    .fluentPut("dataType", c.getDataType()).fluentPut("precision", c.getPrecision())
                    .fluentPut("scale", c.getScale()).fluentPut("nullable", c.isNullable()).fluentPut("defaultValue", c.defaultValue());
            columnsJson.add(columnJson);
        }
        // constraints
        JSONArray constraintsJson = new JSONArray();
        for (Constraint c : getConstraints()) {
            JSONObject constraintJson = new JSONObject();
            constraintJson.fluentPut("name", c.getName()).fluentPut("description", c.getDescription())
                    .fluentPut("type", c.getType()).fluentPut("columnNames", c.getColumnNames());
            constraintsJson.add(constraintJson);
        }
        JSONObject json = new JSONObject();
        json.fluentPut("name", getName()).fluentPut("description", getDescription())
                .fluentPut("columns", columnsJson).fluentPut("constraints", constraintsJson);

        persistence.writeMeta(storagePath, json);
    }
}
