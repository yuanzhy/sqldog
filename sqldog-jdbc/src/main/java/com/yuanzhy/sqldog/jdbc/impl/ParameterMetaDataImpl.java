package com.yuanzhy.sqldog.jdbc.impl;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/21
 */
public class ParameterMetaDataImpl extends AbstractWrapper implements ParameterMetaData {

    @Override
    public int getParameterCount() throws SQLException {
        return 0;
    }

    @Override
    public int isNullable(int param) throws SQLException {
        return 0;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return false;
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int param) throws SQLException {
        return 0;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        return 0;
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return null;
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        return null;
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return 0;
    }
}
