package com.yuanzhy.sqldog.r2dbc;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/9/12
 */
class SqldogRow implements Row {

    private final SqldogRowMetadata rowMetadata;
    private final Object[] data;
    SqldogRow(SqldogRowMetadata rowMetadata, Object[] data) {
        this.rowMetadata = rowMetadata;
        this.data = data;
    }
    @Override
    public RowMetadata getMetadata() {
        return rowMetadata;
    }

    @Override
    public <T> T get(int index, Class<T> type) {
        return type.cast(data[index]);
    }

    @Override
    public <T> T get(String name, Class<T> type) {
        SqldogColumnMetadata cm = rowMetadata.getColumnMetadata(name);
        return get(cm.getIndex(), type);
    }
}
