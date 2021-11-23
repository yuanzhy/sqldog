package com.yuanzhy.sqldog.jdbc.impl;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

import com.yuanzhy.sqldog.core.sql.ParamMetaData;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/21
 */
public class ParameterMetaDataImpl extends AbstractWrapper implements ParameterMetaData {

    private final ParamMetaData[] params;
    public ParameterMetaDataImpl(ParamMetaData[] params) {
        if (params == null) {
            params = new ParamMetaData[0];
        }
        this.params = params;
    }

    @Override
    public int getParameterCount() throws SQLException {
        return params.length;
    }

    @Override
    public int isNullable(int param) throws SQLException {
        return getMetaData(param).getNullable();
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return getMetaData(param).isSigned();
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        return getMetaData(param).getPrecision();
    }

    @Override
    public int getScale(int param) throws SQLException {
        return getMetaData(param).getScale();
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        return getMetaData(param).getParameterType();
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return getMetaData(param).getTypeName();
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        return getMetaData(param).getClassName();
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return getMetaData(param).getMode();
    }

    private ParamMetaData getMetaData(int param) throws SQLException {
        if (param < 1) {
            throw new SQLException("Parameter Index out of range, " + param + " < 1.");
        } else if (param > params.length) {
            throw new SQLException("Parameter Index out of range, " + param + " > " + params.length + ".");
        }
        return params[param - 1];
    }
}
