package com.yuanzhy.sqldog.server.storage.disk;

import com.alibaba.fastjson.JSONObject;
import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.core.Database;
import com.yuanzhy.sqldog.server.core.Persistence;
import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.storage.builder.SchemaBuilder;
import com.yuanzhy.sqldog.server.storage.memory.MemoryDatabase;
import com.yuanzhy.sqldog.server.storage.persistence.PersistenceFactory;

import java.util.List;
import java.util.Map;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/3/29
 */
public class DiskDatabase extends MemoryDatabase implements Database {

    public DiskDatabase(String name, String encoding, String description, String tablespace) {
        super(name, encoding, description, tablespace);
        Persistence persistence = PersistenceFactory.get();
        List<String> schemePaths = persistence.list(getName());
        // 初始化模式
        for (String schemePath : schemePaths) {
            Map<String, Object> metaData = persistence.read(schemePath + "/" + StorageConst.META);
            if (metaData.isEmpty()) {
                continue;
            }
            String schName = (String) metaData.get("name");
            String schDesc = (String) metaData.get("description"); // TODO 可能有循环依赖问题
            super.addSchema(new SchemaBuilder().parent(this).name(schName).description(schDesc).build());
        }
    }

    @Override
    public void addSchema(Schema schema) {
        super.addSchema(schema);
        DiskSchema diskSchema = (DiskSchema) schema;
        diskSchema.initPath(getName());
        diskSchema.persistence();
    }

    @Override
    public void drop() {
        super.drop();
        PersistenceFactory.get().delete(getName());
    }

    @Override
    public void persistence() {
        JSONObject data = new JSONObject();
        data.fluentPut("name", getName()).fluentPut("encoding", getEncoding()).fluentPut("description", getDescription()).fluentPut("tablespace", getTablespace());
        PersistenceFactory.get().write(getName() + "/" + StorageConst.META, data);
    }
}
