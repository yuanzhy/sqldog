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

    private final String storagePath;
    private final Persistence persistence;
    public DiskDatabase(String name, String encoding, String description, String tablespace) {
        super(name, encoding, description, tablespace);
        this.persistence = PersistenceFactory.get();
        this.storagePath = persistence.resolvePath(this);
        List<String> schemePaths = persistence.list(storagePath);
        // 初始化模式
        for (String schemePath : schemePaths) {
            Map<String, Object> metaData = persistence.read(persistence.resolvePath(schemePath, StorageConst.META_NAME));
            if (metaData.isEmpty()) {
                continue;
            }
            String schName = (String) metaData.get("name");
            String schDesc = (String) metaData.get("description");
            super.addSchema(new SchemaBuilder().parent(this).name(schName).description(schDesc).build());
        }
    }

    @Override
    public void addSchema(Schema schema) {
        super.addSchema(schema);
        schema.persistence();
    }

    @Override
    public void drop() {
        super.drop();
        persistence.delete(storagePath);
    }

    @Override
    public void persistence() {
        JSONObject data = new JSONObject();
        data.fluentPut("name", getName()).fluentPut("encoding", getEncoding()).fluentPut("description", getDescription()).fluentPut("tablespace", getTablespace());
        persistence.write(persistence.resolvePath(storagePath, StorageConst.META_NAME), data);
    }
}
