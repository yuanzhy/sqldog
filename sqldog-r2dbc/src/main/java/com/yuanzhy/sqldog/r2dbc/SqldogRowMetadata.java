package com.yuanzhy.sqldog.r2dbc;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import com.yuanzhy.sqldog.core.sql.ColumnMetaData;
import com.yuanzhy.sqldog.core.util.Asserts;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.RowMetadata;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/9/12
 */
class SqldogRowMetadata implements RowMetadata {

    private final List<SqldogColumnMetadata> columnMetadatas;
    SqldogRowMetadata(ColumnMetaData[] columnMetaDatas) {
        Asserts.notNull(columnMetaDatas, "columnMetadatas must not be null");
        this.columnMetadatas = Arrays.stream(columnMetaDatas).map(SqldogColumnMetadata::new).collect(Collectors.toList());
//        this.nameKeyedColumns = new LinkedHashMap<>();
//        for (PostgresqlColumnMetadata columnMetadata : columnMetadatas) {
//            if (!this.nameKeyedColumns.containsKey(columnMetadata.getName())) {
//                this.nameKeyedColumns.put(columnMetadata.getName(), columnMetadata);
//            }
//        }
    }
    @Override
    public SqldogColumnMetadata getColumnMetadata(int index) {
        if (index >= this.columnMetadatas.size()) {
            throw new IndexOutOfBoundsException(String.format("Column index %d is larger than the number of columns %d", index, this.columnMetadatas.size()));
        }

        return this.columnMetadatas.get(index);
    }

    @Override
    public SqldogColumnMetadata getColumnMetadata(String name) {
        Asserts.notNull(name, "name must not be null");

        for (SqldogColumnMetadata metadata : this.columnMetadatas) {

            if (metadata.getName().equalsIgnoreCase(name)) {
                return metadata;
            }
        }

        throw new NoSuchElementException(String.format("Column name '%s' does not exist in column names %s", name, this));
    }

    @Override
    public List<? extends ColumnMetadata> getColumnMetadatas() {
        return Collections.unmodifiableList(this.columnMetadatas);
    }
}
