package com.yuanzhy.sqldog.core.sql;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/20
 */
public class ColumnMetaDataImpl implements ColumnMetaData {

    private static final long serialVersionUID = 1L;

    private final int ordinal; // 0-based
    private final boolean autoIncrement;
    private final boolean caseSensitive;
    private final boolean searchable;
    private final boolean currency;
    private final int nullable;
    private final boolean signed;
    private final int displaySize;
    private final String label;
    private final String columnName;
    private final String schemaName;
    private final int precision;
    private final int scale;
    private final String tableName;
    private final String catalogName;
    private final boolean readOnly;
    private final boolean writable;
    private final boolean definitelyWritable;
    private final String columnClassName;
//    private final AvaticaType type;
    private final int columnType;
    private final String columnTypeName;

    public ColumnMetaDataImpl(
            int ordinal,
            boolean autoIncrement,
            boolean caseSensitive,
            boolean searchable,
            boolean currency,
            int nullable,
            boolean signed,
            int displaySize,
            String label,
            String columnName,
            String schemaName,
            int precision,
            int scale,
            String tableName,
            String catalogName,
//            AvaticaType type,
            boolean readOnly, boolean writable,
            boolean definitelyWritable,
            String columnClassName,
            int columnType,
            String columnTypeName
            ) {
        this.ordinal = ordinal;
        this.autoIncrement = autoIncrement;
        this.caseSensitive = caseSensitive;
        this.searchable = searchable;
        this.currency = currency;
        this.nullable = nullable;
        this.signed = signed;
        this.displaySize = displaySize;
        this.label = label;
        // Per the JDBC spec this should be just columnName.
        // For example, the query
        //     select 1 as x, c as y from t
        // should give columns
        //     (label=x, column=null, table=null)
        //     (label=y, column=c table=t)
        // But DbUnit requires every column to have a name. Duh.
        this.columnName = columnName != null ? columnName : label;
        this.schemaName = schemaName;
        this.precision = precision;
        this.scale = scale;
        this.tableName = tableName;
        this.catalogName = catalogName;
//        this.type = type;
        this.readOnly = readOnly;
        this.writable = writable;
        this.definitelyWritable = definitelyWritable;
        this.columnClassName = columnClassName;
        this.columnType = columnType;
        this.columnTypeName = columnTypeName;
    }

    @Override
    public int getOrdinal() {
        return ordinal;
    }

    @Override
    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    @Override
    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    @Override
    public boolean isSearchable() {
        return searchable;
    }

    @Override
    public boolean isCurrency() {
        return currency;
    }

    @Override
    public int getNullable() {
        return nullable;
    }

    @Override
    public boolean isSigned() {
        return signed;
    }

    @Override
    public int getDisplaySize() {
        return displaySize;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public String getColumnName() {
        return columnName;
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public int getPrecision() {
        return precision;
    }

    @Override
    public int getScale() {
        return scale;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public boolean isWritable() {
        return writable;
    }

    @Override
    public boolean isDefinitelyWritable() {
        return definitelyWritable;
    }

    @Override
    public String getColumnClassName() {
        return columnClassName;
    }

    @Override
    public int getColumnType() {
        return columnType;
    }

    @Override
    public String getColumnTypeName() {
        return columnTypeName;
    }
}
