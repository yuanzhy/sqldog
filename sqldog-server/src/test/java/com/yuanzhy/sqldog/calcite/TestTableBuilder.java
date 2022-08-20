package com.yuanzhy.sqldog.calcite;

import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.storage.builder.TableBuilder;
import com.yuanzhy.sqldog.server.storage.memory.MemorySerial;
import com.yuanzhy.sqldog.server.storage.memory.MemoryTable;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/8/20
 */
class TestTableBuilder extends TableBuilder {

    @Override
    public Table build() {
        Asserts.hasText(name, "table name must not be null");
        //Asserts.notNull(primaryKey, "primaryKey must not be null");
        Asserts.hasEle(columnMap, "table must has column");
        Asserts.notNull(parent, "table parent must not be null");
        if (primaryKey != null) {
            Column pkColumn = columnMap.get(primaryKey.getColumnNames()[0]);
            Asserts.notNull(pkColumn, "primaryKey column '"+primaryKey.getColumnNames()[0]+"' not exists");
            // setSerial
            if (this.serial == null && pkColumn.getDataType().isSerial()) {
                this.serial = new MemorySerial(0, 1); // 默认步长
            }
        }
        return new MemoryTable(parent, name, columnMap, primaryKey, constraint, serial);
    }
}
