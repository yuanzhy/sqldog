package com.yuanzhy.sqldog.core.sql;

import com.yuanzhy.sqldog.core.constant.StatementType;

import java.util.List;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/17
 */
public interface SqlResult {

    public StatementType getType();

    public int getRows();

    public String getSchema();

    public String getTable();

    public String[] getHeaders();

    public List<Object[]> getData();
}
