package com.yuanzhy.sqldog.server.storage.disk;

import com.alibaba.fastjson.JSONObject;
import com.yuanzhy.sqldog.server.core.Base;
import com.yuanzhy.sqldog.server.core.Persistable;
import com.yuanzhy.sqldog.server.core.Persistence;
import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.storage.memory.MemorySchema;
import com.yuanzhy.sqldog.server.storage.persistence.PersistenceFactory;

import java.util.List;
import java.util.Map;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/3/29
 */
public class DiskSchema extends MemorySchema implements Schema, Persistable {
    private final String storagePath;
    private final Persistence persistence;
    protected DiskSchema(Base parent, String schemaPath) {
        super(parent);
        this.persistence = PersistenceFactory.get();
        // 从硬盘中恢复schema
        Map<String, Object> metaData = persistence.readMeta(schemaPath);
        super.rename((String) metaData.get("name"));
        super.setDescription((String) metaData.get("description"));
        this.storagePath = persistence.resolvePath(this);
        // 从硬盘加载子table
        List<String> tablePaths = persistence.list(storagePath);
        for (String tablePath : tablePaths) {
            Table diskTable = new DiskTable(this, tablePath);
            super.addTable(diskTable);
        }
    }
    public DiskSchema(Base parent, String name, String description) {
        super(parent, name, description);
        this.persistence = PersistenceFactory.get();
        this.storagePath = persistence.resolvePath(this);
        this.persistence();
    }

    @Override
    public void setDescription(String description) {
        super.setDescription(description);
        this.persistence();
    }

    @Override
    public void addTable(Table table) {
        super.addTable(table);
        if (table instanceof Persistable) {
            ((Persistable) table).persistence();
        }
    }

    @Override
    public void drop() {
        super.drop();
        persistence.delete(persistence.resolvePath(this));
    }

    @Override
    public void persistence() {
        JSONObject json = new JSONObject();
        json.fluentPut("name", getName()).fluentPut("description", getDescription());
        persistence.writeMeta(storagePath, json);
    }
}
