package com.yuanzhy.sqldog.server.storage.disk;

import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.core.constant.DataType;
import com.yuanzhy.sqldog.server.storage.memory.MemoryColumn;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/3/29
 */
public class DiskColumn extends MemoryColumn implements Column {

    public DiskColumn(String name, DataType dataType, int precision, int scale, boolean nullable, Object defaultValue) {
        super(name, dataType, precision, scale, nullable, defaultValue);
    }

    @Override
    public void setDescription(String description) {
        super.setDescription(description);
//        this.persistence();
    }
}
