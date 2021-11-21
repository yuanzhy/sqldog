package com.yuanzhy.sqldog.core.sql;

import com.yuanzhy.sqldog.core.constant.StatementType;

import java.io.Serializable;
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

    String[] getLabels();

    ColumnMetaData[] getColumns();

    List<Object[]> getData();
}
