package com.yuanzhy.sqldog.server.storage.disk;

import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.storage.memory.MemorySchema;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/3/29
 */
public class DiskSchema extends MemorySchema implements Schema {

    public DiskSchema(String name, String description) {
        super(name, description);
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

    }

    @Override
    public void dropTable(String name) {

    }

    @Override
    public void setDescription(String description) {

    }

    @Override
    public void drop() {

    }
}
