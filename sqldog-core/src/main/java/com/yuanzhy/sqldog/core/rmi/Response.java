package com.yuanzhy.sqldog.core.rmi;

import com.yuanzhy.sqldog.core.sql.SqlResult;

import java.io.Serializable;

/**
 *
 * @author yuanzhy
 * @date 2021-11-18
 */
public interface Response extends Serializable {

    boolean isSuccess();

    String getMessage();

    SqlResult getResult();

    SqlResult[] getResults();
}
