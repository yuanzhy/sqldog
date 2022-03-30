package com.yuanzhy.sqldog.server.storage.disk;

import com.alibaba.fastjson.JSONObject;
import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.core.Base;
import com.yuanzhy.sqldog.server.core.Persistence;
import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.core.constant.ConstraintType;
import com.yuanzhy.sqldog.server.core.constant.DataType;
import com.yuanzhy.sqldog.server.storage.builder.ColumnBuilder;
import com.yuanzhy.sqldog.server.storage.builder.ConstraintBuilder;
import com.yuanzhy.sqldog.server.storage.builder.TableBuilder;
import com.yuanzhy.sqldog.server.storage.memory.MemorySchema;
import com.yuanzhy.sqldog.server.storage.persistence.PersistenceFactory;

import java.util.List;
import java.util.Map;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/3/29
 */
public class DiskSchema extends MemorySchema implements Schema {
    private final String storagePath;
    private final Persistence persistence;
    public DiskSchema(Base parent, String name, String description) {
        super(parent, name, description);
        this.persistence = PersistenceFactory.get();
        this.storagePath = persistence.resolvePath(this);
        List<String> tablePaths = persistence.list(storagePath);
        for (String tablePath : tablePaths) {
            Map<String, Object> map = persistence.read(persistence.resolvePath(tablePath, StorageConst.META_NAME));
            if (map.isEmpty()) {
                continue;
            }
            TableBuilder tb = new TableBuilder().parent(this).name((String)map.get("name")).description((String)map.get("description"));
            List<Map<String, Object>> columns = (List<Map<String, Object>>) map.get("columns");
            for (Map<String, Object> c : columns) {
                tb.addColumn(new ColumnBuilder().name((String)c.get("name")).dataType(DataType.of((String)c.get("dataType")))
                        .precision((int)c.get("precision")).scale((int)c.get("scale")).nullable((boolean)c.get("nullable"))
                        .defaultValue(map.get("defaultValue")).build());
            }
            List<Map<String, Object>> constraints = (List<Map<String, Object>>) map.get("constraints");
            for (Map<String, Object> c : constraints) {
                ConstraintBuilder cb = new ConstraintBuilder().name((String)c.get("name")).type(ConstraintType.valueOf((String)c.get("type")));
                List<String> columnNames = (List<String>) c.get("columnNames");
                for (String columnName : columnNames) {
                    cb.addColumnName(columnName);
                }
                tb.addConstraint(cb.build());
            }
            super.addTable(tb.build());
        }
    }

    @Override
    public void addTable(Table table) {
        super.addTable(table);
        table.persistence();
    }

    @Override
    public void drop() {
        super.drop();
        persistence.delete(persistence.resolvePath(this));
    }

    @Override
    public void persistence() {
        String relPath = persistence.resolvePath(this, StorageConst.META_NAME);
        JSONObject json = new JSONObject();
        json.fluentPut("name", getName()).fluentPut("description", getDescription());
        persistence.write(relPath, json);
    }
}
