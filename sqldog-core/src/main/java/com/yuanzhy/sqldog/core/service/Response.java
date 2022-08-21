package com.yuanzhy.sqldog.core.service;

import java.io.Serializable;

import com.yuanzhy.sqldog.core.sql.SqlResult;

/**
 *
 * @author yuanzhy
 * @date 2021-11-18
 */
public interface Response extends Serializable {

    boolean isSuccess();

    String getMessage();

    SqlResult[] getResults();

    default SqlResult getResult() {
        SqlResult[] results = getResults();
        return results == null ? null : results[0];
    }
}
