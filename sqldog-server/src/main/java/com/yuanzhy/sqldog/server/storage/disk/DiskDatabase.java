package com.yuanzhy.sqldog.server.storage.disk;

import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONObject;
import com.yuanzhy.sqldog.server.core.Database;
import com.yuanzhy.sqldog.server.core.Persistable;
import com.yuanzhy.sqldog.server.core.Persistence;
import com.yuanzhy.sqldog.server.sql.adapter.CalciteSchema;
import com.yuanzhy.sqldog.server.storage.memory.MemoryDatabase;
import com.yuanzhy.sqldog.server.storage.persistence.PersistenceFactory;
import com.yuanzhy.sqldog.server.util.Calcites;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/3/29
 */
public class DiskDatabase extends MemoryDatabase implements Database, Persistable {

    private final String storagePath;
    private final Persistence persistence;

    /**
     * 用于启动数据库的时候从硬盘恢复数据库
     * @param databasePath 数据库路径
     */
    public DiskDatabase(String databasePath) {
        super();
        this.persistence = PersistenceFactory.get();
        // 从硬盘中加载database
        Map<String, Object> map = persistence.readMeta(databasePath); // 数据库名称就是相对path
        super.rename((String)map.get("name"));
        super.setDescription((String)map.get("description"));
        super.encoding = (String)map.get("encoding");
        super.tablespace = (String)map.get("tablespace");
        this.storagePath = persistence.resolvePath(this);
        // 从硬盘中初始化子schema
        List<String> schemePaths = persistence.list(databasePath);
        for (String schemePath : schemePaths) {
            DiskSchema diskSchema = new DiskSchema(this, schemePath);
            this.addSchema(diskSchema);
            // TODO 此处野路子先调用一下，后续看看怎么优化 2022-04-10
            CalciteSchema calciteSchema = new CalciteSchema(diskSchema);
            Calcites.getRootSchema().add(diskSchema.getName(), calciteSchema);
            diskSchema.addObserver(calciteSchema);
        }
    }

    /**
     * 用户通过API创建数据库对象，此时需要手动持久化以下
     * @param name 数据库名称
     * @param encoding 编码
     * @param description 描述
     * @param tablespace  表空间
     */
    public DiskDatabase(String name, String encoding, String description, String tablespace) {
        super(name, encoding, description, tablespace);
        this.persistence = PersistenceFactory.get();
        this.storagePath = persistence.resolvePath(this);
        this.persistence(); // 手动持久化
    }

    @Override
    public void setDescription(String description) {
        super.setDescription(description);
        this.persistence();
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
        persistence.writeMeta(storagePath, data);
    }
}
