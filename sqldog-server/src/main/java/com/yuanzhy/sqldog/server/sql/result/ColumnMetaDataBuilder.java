package com.yuanzhy.sqldog.server.sql.result;

import com.yuanzhy.sqldog.core.sql.ColumnMetaData;
import com.yuanzhy.sqldog.core.sql.ColumnMetaDataImpl;
import com.yuanzhy.sqldog.core.util.Asserts;
import org.apache.commons.lang3.StringUtils;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/20
 */
public class ColumnMetaDataBuilder {
    private int ordinal;
    private boolean autoIncrement;
    private boolean caseSensitive;
    private boolean searchable;
    private boolean currency;
    private int nullable;
    private boolean signed;
    private int displaySize;
    private String label;
    private String columnName;
    private String schemaName;
    private int precision;
    private int scale;
    private String tableName;
    private String catalogName;
    private boolean readOnly;
    private boolean writable;
    private boolean definitelyWritable;
    private String columnClassName;
    private int columnType;
    private String columnTypeName;

    public ColumnMetaDataBuilder ordinal(int ordinal) {
        this.ordinal = ordinal;
        return this;
    }

    public ColumnMetaDataBuilder autoIncrement(boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
        return this;
    }

    public ColumnMetaDataBuilder caseSensitive(boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
        return this;
    }

    public ColumnMetaDataBuilder searchable(boolean searchable) {
        this.searchable = searchable;
        return this;
    }

    public ColumnMetaDataBuilder currency(boolean currency) {
        this.currency = currency;
        return this;
    }

    public ColumnMetaDataBuilder nullable(int nullable) {
        this.nullable = nullable;
        return this;
    }

    public ColumnMetaDataBuilder signed(boolean signed) {
        this.signed = signed;
        return this;
    }

    public ColumnMetaDataBuilder displaySize(int displaySize) {
        this.displaySize = displaySize;
        return this;
    }

    public ColumnMetaDataBuilder label(String label) {
        this.label = label;
        return this;
    }

    public ColumnMetaDataBuilder columnName(String columnName) {
        this.columnName = columnName;
        return this;
    }

    public ColumnMetaDataBuilder schemaName(String schemaName) {
        this.schemaName = schemaName;
        return this;
    }

    public ColumnMetaDataBuilder precision(int precision) {
        this.precision = precision;
        return this;
    }

    public ColumnMetaDataBuilder scale(int scale) {
        this.scale = scale;
        return this;
    }

    public ColumnMetaDataBuilder tableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public ColumnMetaDataBuilder catalogName(String catalogName) {
        this.catalogName = catalogName;
        return this;
    }

    public ColumnMetaDataBuilder readOnly(boolean readOnly) {
        this.readOnly = readOnly;
        return this;
    }

    public ColumnMetaDataBuilder writable(boolean writable) {
        this.writable = writable;
        return this;
    }

    public ColumnMetaDataBuilder definitelyWritable(boolean definitelyWritable) {
        this.definitelyWritable = definitelyWritable;
        return this;
    }

    public ColumnMetaDataBuilder columnClassName(String columnClassName) {
        this.columnClassName = columnClassName;
        return this;
    }

    public ColumnMetaDataBuilder columnType(int columnType) {
        this.columnType = columnType;
        return this;
    }

    public ColumnMetaDataBuilder columnTypeName(String columnTypeName) {
        this.columnTypeName = columnTypeName;
        return this;
    }

    public ColumnMetaData build() {
        Asserts.hasText(columnName, "'columnName' cannot be empty");
        if (StringUtils.isEmpty(label)) {
            label = columnName;
        }
        return new ColumnMetaDataImpl(ordinal, autoIncrement, caseSensitive, searchable, currency, nullable, signed,
                displaySize, label, columnName, schemaName, precision, scale, tableName, catalogName, readOnly, writable,
                definitelyWritable, columnClassName, columnType, columnTypeName);
    }
}
