package com.yuanzhy.sqldog.core.sql;

import java.io.Serializable;

/**
 *
 * @author yuanzhy
 * @date 2021-11-22
 */
public interface ParamMetaData extends Serializable {

    boolean isSigned();

    int getPrecision();

    int getScale();

    int getParameterType();

    String getTypeName();

    String getClassName();

    int getMode();
}
