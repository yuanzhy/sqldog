package com.yuanzhy.sqldog.core;

import com.yuanzhy.sqldog.core.constant.ConstraintType;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public interface Constraint extends Base {

    ConstraintType getType();

    String[] getColumnNames();
}
