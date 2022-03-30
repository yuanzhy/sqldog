package com.yuanzhy.sqldog.server.storage.disk;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.core.Base;
import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.core.Constraint;
import com.yuanzhy.sqldog.server.core.Persistence;
import com.yuanzhy.sqldog.server.core.Serial;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.storage.memory.MemoryTable;
import com.yuanzhy.sqldog.server.storage.persistence.PersistenceFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/3/29
 */
public class DiskTable extends MemoryTable implements Table {
    private final String storagePath;
    private final Persistence persistence;
    public DiskTable(Base parent, String name, Map<String, Column> columnMap, Constraint primaryKey, Set<Constraint> constraint, Serial serial) {
        super(parent, name, columnMap, primaryKey, constraint, serial);
        this.persistence = PersistenceFactory.get();
        this.storagePath = persistence.resolvePath(this);
    }

    @Override
    public List<Object[]> getData() {
        return null;
    }

    @Override
    public void addColumn(Column column) {

    }

    @Override
    public void dropColumn(String columnName) {

    }

    @Override
    public void truncate() {
        super.truncate();
        persistence.delete(persistence.resolvePath(storagePath, StorageConst.TABLE_DATA_PATH));
        persistence.delete(persistence.resolvePath(storagePath, StorageConst.TABLE_INDEX_PATH));
    }

    @Override
    public void drop() {
        super.drop();
        persistence.delete(storagePath);
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

        persistence.write(persistence.resolvePath(storagePath, StorageConst.META_NAME), json);
    }
}
