package com.yuanzhy.sqldog.core.sql;

/**
 *
 * @author yuanzhy
 * @date 2021-11-22
 */
public class ParamMetaDataImpl implements ParamMetaData {
    private static final long serialVersionUID = 1L;

    private final boolean signed;
    private final int precision;
    private final int scale;
    private final int parameterType;
    private final String typeName;
    private final String className;
    private final int mode;
    private final int nullable;

    public ParamMetaDataImpl(boolean signed, int precision, int scale, int parameterType,
            String typeName, String className, int mode, int nullable) {
        this.signed = signed;
        this.precision = precision;
        this.scale = scale;
        this.parameterType = parameterType;
        this.typeName = typeName;
        this.className = className;
        this.mode = mode;
        this.nullable = nullable;
    }

    public boolean isSigned() {
        return signed;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public int getParameterType() {
        return parameterType;
    }

    public String getTypeName() {
        return typeName;
    }

    public String getClassName() {
        return className;
    }

    public int getMode() {
        return mode;
    }

    public int getNullable() {
        return nullable;
    }
}
