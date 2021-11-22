package com.yuanzhy.sqldog.core.sql;

import com.yuanzhy.sqldog.core.constant.StatementType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/17
 */
public interface SqlResult extends Serializable {

    StatementType getType();

    long getRows();

    String getSchema();

    String getTable();

    ParamMetaData[] getParams();

    ColumnMetaData[] getColumns();

    List<Object[]> getData();

    default String[] getLabels() {
        ColumnMetaData[] columns = getColumns();
        if (columns == null) {
            return null;
        }
        return Arrays.stream(columns).map(ColumnMetaData::getLabel).toArray(String[]::new);
    }
}
