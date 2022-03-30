package com.yuanzhy.sqldog.server.storage.disk;

import com.alibaba.fastjson.JSONObject;
import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.server.common.StorageConst;
import com.yuanzhy.sqldog.server.core.Base;
import com.yuanzhy.sqldog.server.core.Persistence;
import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.storage.memory.MemorySchema;
import com.yuanzhy.sqldog.server.storage.persistence.PersistenceFactory;

import java.io.File;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/3/29
 */
public class DiskSchema extends MemorySchema implements Schema {
    /** 相对路径: 相对于数据根目录的路径 */
    private transient String relPath;

    public DiskSchema(Base parent, String name, String description) {
        super(parent, name, description);
        Persistence persistence = PersistenceFactory.get();
//        persistence.list();
        // TODO 从硬盘读出所有的表
//        Table table = new TableBuilder().name("PERSON")
//                .addColumn(new ColumnBuilder().name("ID").dataType(DataType.INT).nullable(false).build())
//                .addColumn(new ColumnBuilder().name("NAME").dataType(DataType.VARCHAR).precision(50).build())
//                .addColumn(new ColumnBuilder().name("AGE").dataType(DataType.INT).build())
//                .addConstraint(new ConstraintBuilder().type(ConstraintType.PRIMARY_KEY).addColumnName("ID").build())
//                .build();
//        super.addTable(table);
    }

    @Override
    public void addTable(Table table) {
        super.addTable(table);
        DiskTable diskTable = (DiskTable) table;
        diskTable.initPath(relPath);
        diskTable.persistence();
    }

    @Override
    public void drop() {
        super.drop();
        PersistenceFactory.get().delete(relPath);
    }

    @Override
    public void persistence() {
        Asserts.hasText(relPath, "The path must not null");
        File metaFile = new File(relPath, StorageConst.META);
        JSONObject json = new JSONObject();
        json.fluentPut("name", getName()).fluentPut("description", getDescription());
        PersistenceFactory.get().write(metaFile.getAbsolutePath(), json);
    }

    void initPath(String basePath) {
        this.relPath = basePath + "/" + getName();
    }
}
