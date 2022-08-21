package com.yuanzhy.sqldog.jdbc.impl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.yuanzhy.sqldog.core.constant.Consts;
import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.constant.TableType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.core.sql.SqlResultImpl;
import com.yuanzhy.sqldog.core.util.SqlUtil;
import com.yuanzhy.sqldog.jdbc.Driver;
import com.yuanzhy.sqldog.jdbc.SqldogConnection;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/21
 */
class DatabaseMetaDataImpl extends AbstractWrapper implements DatabaseMetaData {

    private static final DbmdResultSet EMPTY_RS = new DbmdResultSet(new SqlResultImpl(StatementType.DQL, 0, null, null, null, null, null, null)) {
        @Override
        public void close() throws SQLException {
            // ignore
        }
    };

    // SQL:92 reserved words from 'ANSI X3.135-1992, January 4, 1993'
    //private static final String[] SQL92_KEYWORDS = new String[] { "ABSOLUTE", "ACTION", "ADD", "ALL", "ALLOCATE", "ALTER", "AND", "ANY", "ARE", "AS", "ASC",
    //        "ASSERTION", "AT", "AUTHORIZATION", "AVG", "BEGIN", "BETWEEN", "BIT", "BIT_LENGTH", "BOTH", "BY", "CASCADE", "CASCADED", "CASE", "CAST", "CATALOG",
    //        "CHAR", "CHARACTER", "CHARACTER_LENGTH", "CHAR_LENGTH", "CHECK", "CLOSE", "COALESCE", "COLLATE", "COLLATION", "COLUMN", "COMMIT", "CONNECT",
    //        "CONNECTION", "CONSTRAINT", "CONSTRAINTS", "CONTINUE", "CONVERT", "CORRESPONDING", "COUNT", "CREATE", "CROSS", "CURRENT", "CURRENT_DATE",
    //        "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", "DATE", "DAY", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DEFERRABLE",
    //        "DEFERRED", "DELETE", "DESC", "DESCRIBE", "DESCRIPTOR", "DIAGNOSTICS", "DISCONNECT", "DISTINCT", "DOMAIN", "DOUBLE", "DROP", "ELSE", "END",
    //        "END-EXEC", "ESCAPE", "EXCEPT", "EXCEPTION", "EXEC", "EXECUTE", "EXISTS", "EXTERNAL", "EXTRACT", "FALSE", "FETCH", "FIRST", "FLOAT", "FOR",
    //        "FOREIGN", "FOUND", "FROM", "FULL", "GET", "GLOBAL", "GO", "GOTO", "GRANT", "GROUP", "HAVING", "HOUR", "IDENTITY", "IMMEDIATE", "IN", "INDICATOR",
    //        "INITIALLY", "INNER", "INPUT", "INSENSITIVE", "INSERT", "INT", "INTEGER", "INTERSECT", "INTERVAL", "INTO", "IS", "ISOLATION", "JOIN", "KEY",
    //        "LANGUAGE", "LAST", "LEADING", "LEFT", "LEVEL", "LIKE", "LOCAL", "LOWER", "MATCH", "MAX", "MIN", "MINUTE", "MODULE", "MONTH", "NAMES", "NATIONAL",
    //        "NATURAL", "NCHAR", "NEXT", "NO", "NOT", "NULL", "NULLIF", "NUMERIC", "OCTET_LENGTH", "OF", "ON", "ONLY", "OPEN", "OPTION", "OR", "ORDER", "OUTER",
    //        "OUTPUT", "OVERLAPS", "PAD", "PARTIAL", "POSITION", "PRECISION", "PREPARE", "PRESERVE", "PRIMARY", "PRIOR", "PRIVILEGES", "PROCEDURE", "PUBLIC",
    //        "READ", "REAL", "REFERENCES", "RELATIVE", "RESTRICT", "REVOKE", "RIGHT", "ROLLBACK", "ROWS", "SCHEMA", "SCROLL", "SECOND", "SECTION", "SELECT",
    //        "SESSION", "SESSION_USER", "SET", "SIZE", "SMALLINT", "SOME", "SPACE", "SQL", "SQLCODE", "SQLERROR", "SQLSTATE", "SUBSTRING", "SUM", "SYSTEM_USER",
    //        "TABLE", "TEMPORARY", "THEN", "TIME", "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TRAILING", "TRANSACTION", "TRANSLATE", "TRANSLATION",
    //        "TRIM", "TRUE", "UNION", "UNIQUE", "UNKNOWN", "UPDATE", "UPPER", "USAGE", "USER", "USING", "VALUE", "VALUES", "VARCHAR", "VARYING", "VIEW", "WHEN",
    //        "WHENEVER", "WHERE", "WITH", "WORK", "WRITE", "YEAR", "ZONE" };

    // SQL:2003 reserved words from 'ISO/IEC 9075-2:2003 (E), 2003-07-25'
    private static final String[] SQL2003_KEYWORDS = new String[] { "ABS", "ALL", "ALLOCATE", "ALTER", "AND", "ANY", "ARE", "ARRAY", "AS", "ASENSITIVE",
            "ASYMMETRIC", "AT", "ATOMIC", "AUTHORIZATION", "AVG", "BEGIN", "BETWEEN", "BIGINT", "BINARY", "BLOB", "BOOLEAN", "BOTH", "BY", "CALL", "CALLED",
            "CARDINALITY", "CASCADED", "CASE", "CAST", "CEIL", "CEILING", "CHAR", "CHARACTER", "CHARACTER_LENGTH", "CHAR_LENGTH", "CHECK", "CLOB", "CLOSE",
            "COALESCE", "COLLATE", "COLLECT", "COLUMN", "COMMIT", "CONDITION", "CONNECT", "CONSTRAINT", "CONVERT", "CORR", "CORRESPONDING", "COUNT",
            "COVAR_POP", "COVAR_SAMP", "CREATE", "CROSS", "CUBE", "CUME_DIST", "CURRENT", "CURRENT_DATE", "CURRENT_DEFAULT_TRANSFORM_GROUP", "CURRENT_PATH",
            "CURRENT_ROLE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_TRANSFORM_GROUP_FOR_TYPE", "CURRENT_USER", "CURSOR", "CYCLE", "DATE", "DAY",
            "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DELETE", "DENSE_RANK", "DEREF", "DESCRIBE", "DETERMINISTIC", "DISCONNECT", "DISTINCT",
            "DOUBLE", "DROP", "DYNAMIC", "EACH", "ELEMENT", "ELSE", "END", "END-EXEC", "ESCAPE", "EVERY", "EXCEPT", "EXEC", "EXECUTE", "EXISTS", "EXP",
            "EXTERNAL", "EXTRACT", "FALSE", "FETCH", "FILTER", "FLOAT", "FLOOR", "FOR", "FOREIGN", "FREE", "FROM", "FULL", "FUNCTION", "FUSION", "GET",
            "GLOBAL", "GRANT", "GROUP", "GROUPING", "HAVING", "HOLD", "HOUR", "IDENTITY", "IN", "INDICATOR", "INNER", "INOUT", "INSENSITIVE", "INSERT", "INT",
            "INTEGER", "INTERSECT", "INTERSECTION", "INTERVAL", "INTO", "IS", "JOIN", "LANGUAGE", "LARGE", "LATERAL", "LEADING", "LEFT", "LIKE", "LN", "LOCAL",
            "LOCALTIME", "LOCALTIMESTAMP", "LOWER", "MATCH", "MAX", "MEMBER", "MERGE", "METHOD", "MIN", "MINUTE", "MOD", "MODIFIES", "MODULE", "MONTH",
            "MULTISET", "NATIONAL", "NATURAL", "NCHAR", "NCLOB", "NEW", "NO", "NONE", "NORMALIZE", "NOT", "NULL", "NULLIF", "NUMERIC", "OCTET_LENGTH", "OF",
            "OLD", "ON", "ONLY", "OPEN", "OR", "ORDER", "OUT", "OUTER", "OVER", "OVERLAPS", "OVERLAY", "PARAMETER", "PARTITION", "PERCENTILE_CONT",
            "PERCENTILE_DISC", "PERCENT_RANK", "POSITION", "POWER", "PRECISION", "PREPARE", "PRIMARY", "PROCEDURE", "RANGE", "RANK", "READS", "REAL",
            "RECURSIVE", "REF", "REFERENCES", "REFERENCING", "REGR_AVGX", "REGR_AVGY", "REGR_COUNT", "REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE", "REGR_SXX",
            "REGR_SXY", "REGR_SYY", "RELEASE", "RESULT", "RETURN", "RETURNS", "REVOKE", "RIGHT", "ROLLBACK", "ROLLUP", "ROW", "ROWS", "ROW_NUMBER", "SAVEPOINT",
            "SCOPE", "SCROLL", "SEARCH", "SECOND", "SELECT", "SENSITIVE", "SESSION_USER", "SET", "SIMILAR", "SMALLINT", "SOME", "SPECIFIC", "SPECIFICTYPE",
            "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "SQRT", "START", "STATIC", "STDDEV_POP", "STDDEV_SAMP", "SUBMULTISET", "SUBSTRING", "SUM",
            "SYMMETRIC", "SYSTEM", "SYSTEM_USER", "TABLE", "TABLESAMPLE", "THEN", "TIME", "TIMESTAMP", "TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TRAILING",
            "TRANSLATE", "TRANSLATION", "TREAT", "TRIGGER", "TRIM", "TRUE", "UESCAPE", "UNION", "UNIQUE", "UNKNOWN", "UNNEST", "UPDATE", "UPPER", "USER",
            "USING", "VALUE", "VALUES", "VARCHAR", "VARYING", "VAR_POP", "VAR_SAMP", "WHEN", "WHENEVER", "WHERE", "WIDTH_BUCKET", "WINDOW", "WITH", "WITHIN",
            "WITHOUT", "YEAR" };

    private static final String[] SQLDOG_KEYWORDS = new String[] {"TINYINT", "TEXT", "JSON", "BYTEA", "SERIAL", "SEARCH_PATH", "USE"};

    private final SqldogConnection connection;
    private final String host;
    private final int port;
    private final String schema;
    private final Properties info;
    private final String version;
    DatabaseMetaDataImpl(SqldogConnection connection, String host, int port, String schema, Properties info, String version) {
        this.connection = connection;
        this.host = host;
        this.port = port;
        this.schema = schema;
        this.info = info;
        this.version = version;
    }
    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return false;
    }

    @Override
    public String getURL() throws SQLException {
        if (Util.isEmpty(schema)) {
            return String.format("jdbc:sqldog://%s:%s", host, port);
        } else {
            return String.format("jdbc:sqldog://%s:%s/%s", host, port, schema);
        }
    }

    @Override
    public String getUserName() throws SQLException {
        return info.getProperty(Driver.USER_PROPERTY_KEY);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return "sqldog";
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return version;
    }

    @Override
    public String getDriverName() throws SQLException {
        return Driver.NAME;
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return Driver.VERSION;
    }

    @Override
    public int getDriverMajorVersion() {
        return Integer.parseInt(Util.substringBefore(Driver.VERSION, "."));
    }

    @Override
    public int getDriverMinorVersion() {
        String s = Util.substringAfter(Driver.VERSION, ".");
        if (s.contains(".")) {
            s = Util.substringBefore(s, ".");
        }
        return Integer.parseInt(s);
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "\"";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        Set<String> set = new TreeSet<>();
        Collections.addAll(set, SQL2003_KEYWORDS);
        Collections.addAll(set, SQLDOG_KEYWORDS);
        return set.stream().collect(Collectors.joining(","));
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return "ABS,ACOS,ASIN,ATAN,ATAN2,BIT_COUNT,CEILING,COS,COT,DEGREES,EXP,FLOOR,LOG,LOG10,MAX,MIN,MOD,PI,POW,"
                + "POWER,RADIANS,RAND,ROUND,SIN,SQRT,TAN,TRUNCATE";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return "LENGTH,CHAR_LENGTH,CONCAT,CONCAT_WS,TO_CHAR,OCTET_LENGTH,BTRIM,LTRIM,RTRIM,REVERSE,REPEAT,CHR,MD5,UPPER,LOWER,SUBSTRING";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return "CURRENT_DATE,CURRENT_TIME,CURRENT_TIMESTAMP,USER,CURRENT_USER,SESSION_USER,SYSTEM_USER";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return "NOW,CURRENT_DATE,CURRENT_TIME,CURRENT_TIMESTAMP,DATE_TRUNC,DAYOFWEEK,WEEKDAY,DAYOFMONTH,DAYOFYEAR,MONTH,DAYNAME,MONTHNAME,QUARTER,WEEK,YEAR,HOUR,MINUTE,SECOND";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "schema";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "database";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return null;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 16777208;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 16777208;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 64;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 64;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 16;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 64;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 256;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 512;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 64;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 256;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 64;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 64;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 64;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return Integer.MAX_VALUE - 8;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return true;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 65531;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 64;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 256;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 64;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return EMPTY_RS;
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return EMPTY_RS;
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        if ("".equals(catalog) || "".equals(schemaPattern)) { // 表不支持没有库和模式，所以直接返回空结果集
            return EMPTY_RS;
        }
        schemaPattern = handlePattern(schemaPattern);
        tableNamePattern = handlePattern(tableNamePattern);
        StringBuilder builder = new StringBuilder("select * from ").append(Consts.SYSTABLE_PREFIX).append("TABLE");
        int baseLen = builder.length();
        if (catalog != null) {
            builder.append(" and TABLE_CAT='").append(SqlUtil.escape(catalog)).append("'");
        }
        if (schemaPattern != null) {
            builder.append(" and TABLE_SCHEM = '").append(SqlUtil.escape(schemaPattern)).append("'");
        }
        if (Util.isNotEmpty(tableNamePattern)) {
            builder.append(" and TABLE_NAME = '").append(SqlUtil.escape(tableNamePattern)).append("'");
        }
        if (types != null && types.length > 0) {
            List<String> legalTypes = Arrays.stream(TableType.values()).map(TableType::getName).collect(Collectors.toList());
            String in = Arrays.stream(types).filter(legalTypes::contains).map(t -> "'"+t+"'").collect(Collectors.joining(",", "(", ")"));
            builder.append(" and TABLE_TYPE in ").append(in);
        }
        if (builder.length() > baseLen) {
            builder.replace(baseLen+1, baseLen+4, "where");
        }
        builder.append(" order by TABLE_TYPE,TABLE_CAT,TABLE_SCHEM,TABLE_NAME");
        return executeCommand(builder.toString());
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return executeCommand("SHOW DATABASES");
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return executeCommand("SHOW TABLETYPES");
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        if ("".equals(catalog) || "".equals(schemaPattern)) {
            return EMPTY_RS;
        }
        schemaPattern = handlePattern(schemaPattern);
        tableNamePattern = handlePattern(tableNamePattern);
        columnNamePattern = handlePattern(columnNamePattern);
        StringBuilder builder = new StringBuilder("select * from ").append(Consts.SYSTABLE_PREFIX).append("COLUMN");
        int baseLen = builder.length();
        if (catalog != null) {
            builder.append(" and TABLE_CAT='").append(SqlUtil.escape(catalog)).append("'");
        }
        if (schemaPattern != null) {
            builder.append(" and TABLE_SCHEM = '").append(SqlUtil.escape(schemaPattern)).append("'");
        }
        if (Util.isNotEmpty(tableNamePattern)) {
            builder.append(" and TABLE_NAME = '").append(SqlUtil.escape(tableNamePattern)).append("'");
        }
        if (Util.isNotEmpty(columnNamePattern)) {
            builder.append(" and COLUMN_NAME = '").append(SqlUtil.escape(columnNamePattern)).append("'");
        }
        if (builder.length() > baseLen) {
            builder.replace(baseLen+1, baseLen+4, "where");
        }
        builder.append(" order by TABLE_CAT,TABLE_SCHEM,TABLE_NAME,ORDINAL_POSITION");
        return executeCommand(builder.toString());
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return EMPTY_RS; // TODO 暂不支持权限
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return EMPTY_RS; // TODO 暂不支持权限
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
//        if (table == null) {
//            throw new SQLException("Table not specified.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
//        }
        return EMPTY_RS; // 不支持隐式唯一列
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return EMPTY_RS; // 不支持隐式版本列
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        if ("".equals(catalog) || "".equals(schema)) {
            return EMPTY_RS;
        }
        StringBuilder builder = new StringBuilder("select * from ").append(Consts.SYSTABLE_PREFIX).append("PRIMARYKEY");
        int baseLen = builder.length();
        if (catalog != null) {
            builder.append(" and TABLE_CAT='").append(SqlUtil.escape(catalog)).append("'");
        }
        if (schema != null) {
            builder.append(" and TABLE_SCHEM='").append(SqlUtil.escape(schema)).append("'");
        }
        if (Util.isNotEmpty(table)) {
            builder.append(" and TABLE_NAME='").append(SqlUtil.escape(table)).append("'");
        }
        if (builder.length() > baseLen) {
            builder.replace(baseLen+1, baseLen+4, "where");
        }
        builder.append(" order by COLUMN_NAME");
        return executeCommand(builder.toString());
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return EMPTY_RS; // TODO 暂不支持外键约束
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return EMPTY_RS; // TODO 暂不支持外键约束
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return EMPTY_RS; // TODO 暂不支持外键约束
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return executeCommand("SHOW TYPEINFO");
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        return EMPTY_RS; // TODO 暂不支持索引
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false; // TODO UpdatedResultSetImpl 实现后再议
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false; // TODO UpdatedResultSetImpl 实现后再议
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false; // TODO UpdatedResultSetImpl 实现后再议
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false; // TODO UpdatedResultSetImpl 实现后再议
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false; // TODO UpdatedResultSetImpl 实现后再议
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false; // TODO UpdatedResultSetImpl 实现后再议
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return EMPTY_RS;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return true;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return EMPTY_RS;
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return EMPTY_RS;
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return EMPTY_RS;
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return connection.getHoldability();
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return Integer.parseInt(Util.substringBefore(version, "."));
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        String s = Util.substringAfter(version, ".");
        if (s.contains(".")) {
            s = Util.substringBefore(s, ".");
        }
        return Integer.parseInt(s);
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return 1;
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        if ("".equals(catalog)) {
            return EMPTY_RS;
        }
        schemaPattern = handlePattern(schemaPattern);
        StringBuilder builder = new StringBuilder("select * from ").append(Consts.SYSTABLE_PREFIX).append("SCHEMA");
        int baseLen = builder.length();
        if (catalog != null) {
            builder.append(" and TABLE_CATALOG='").append(SqlUtil.escape(catalog)).append("'");
        }
        if (Util.isNotEmpty(schemaPattern)) {
            builder.append(" and TABLE_SCHEM = '").append(SqlUtil.escape(schemaPattern)).append("'");
        }
        if (builder.length() > baseLen) {
            builder.replace(baseLen+1, baseLen+4, "where");
        }
        builder.append(" order by TABLE_CATALOG,TABLE_SCHEM");
        return executeCommand(builder.toString());
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return EMPTY_RS; // support any
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        return executeCommand("SHOW FUNCTIONS");
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        return EMPTY_RS; // TODO gaota
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return EMPTY_RS; // not supported
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return true;
    }

    private ResultSet executeCommand(String cmd) throws SQLException {
        return connection.createStatement().executeQuery(cmd);
    }

    private String handlePattern(String pattern) {
        if ("%".equals(pattern)) {
            return null;
        }
        return pattern;
    }

    private static class DbmdResultSet extends ResultSetImpl {

        DbmdResultSet(SqlResult sqlResult) {
            super(null, ResultSet.FETCH_FORWARD, 0, sqlResult);
        }

        @Override
        public int getType() throws SQLException {
            return TYPE_FORWARD_ONLY;
        }

        @Override
        public int getConcurrency() throws SQLException {
            return CONCUR_READ_ONLY;
        }

        @Override
        public int getHoldability() throws SQLException {
            return HOLD_CURSORS_OVER_COMMIT;
        }

        @Override
        protected void checkScrollType() throws SQLException {
            checkClosed();
        }
    }
}
