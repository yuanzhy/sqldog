package com.yuanzhy.sqldog.dialect.hibernate;

import org.hibernate.cfg.Environment;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.function.NoArgSQLFunction;
import org.hibernate.dialect.function.StandardSQLFunction;
import org.hibernate.dialect.function.VarArgsSQLFunction;
import org.hibernate.dialect.pagination.AbstractLimitHandler;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.dialect.pagination.LimitHelper;
import org.hibernate.engine.spi.RowSelection;
import org.hibernate.exception.spi.TemplatedViolatedConstraintNameExtracter;
import org.hibernate.exception.spi.ViolatedConstraintNameExtracter;
import org.hibernate.hql.spi.id.IdTableSupportStandardImpl;
import org.hibernate.hql.spi.id.MultiTableBulkIdStrategy;
import org.hibernate.hql.spi.id.local.AfterUseAction;
import org.hibernate.hql.spi.id.local.LocalTemporaryTableBulkIdStrategy;
import org.hibernate.internal.util.JdbcExceptionHelper;
import org.hibernate.type.StandardBasicTypes;
import org.hibernate.type.descriptor.sql.BlobTypeDescriptor;
import org.hibernate.type.descriptor.sql.ClobTypeDescriptor;
import org.hibernate.type.descriptor.sql.SqlTypeDescriptor;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 *
 * @author yuanzhy
 * @date 2021-11-24
 */
public class SqldogDialect extends Dialect {

    private static final AbstractLimitHandler LIMIT_HANDLER = new AbstractLimitHandler() {
        @Override
        public String processSql(String sql, RowSelection selection) {
            final boolean hasOffset = LimitHelper.hasFirstRow( selection );
            return sql + (hasOffset ? " limit ? offset ?" : " limit ?");
        }

        @Override
        public boolean supportsLimit() {
            return true;
        }

        @Override
        public boolean bindLimitParametersInReverseOrder() {
            return true;
        }
    };

    /**
     * Constructs a PostgreSQL81Dialect
     */
    public SqldogDialect() {
        super();
        registerColumnType( Types.BOOLEAN, "boolean" );
        registerColumnType( Types.BIGINT, "bigint" );
        registerColumnType( Types.SMALLINT, "smallint" );
        registerColumnType( Types.TINYINT, "tinyint" );
        registerColumnType( Types.INTEGER, "int" );
        registerColumnType( Types.CHAR, "char(1)" );
        registerColumnType( Types.VARCHAR, "varchar($l)" );
        registerColumnType( Types.FLOAT, "float" );
        registerColumnType( Types.DOUBLE, "double" );
        registerColumnType( Types.DATE, "date" );
        registerColumnType( Types.TIME, "time" );
        registerColumnType( Types.TIMESTAMP, "timestamp" );
        registerColumnType( Types.VARBINARY, "bytea" );
        registerColumnType( Types.BINARY, "bytea" );
        registerColumnType( Types.LONGVARCHAR, "text" );
        registerColumnType( Types.LONGVARBINARY, "bytea" );
        registerColumnType( Types.CLOB, "text" );
        registerColumnType( Types.BLOB, "bytea" );
        registerColumnType( Types.NUMERIC, "numeric($p, $s)" );
        registerColumnType( Types.DECIMAL, "decimal($p, $s)" );
        registerColumnType( Types.JAVA_OBJECT, "json" );

        registerFunction( "random", new NoArgSQLFunction("random", StandardBasicTypes.DOUBLE) );
        registerFunction( "rand", new NoArgSQLFunction("random", StandardBasicTypes.DOUBLE) );
        registerFunction( "uuid", new NoArgSQLFunction("uuid", StandardBasicTypes.STRING) );

        registerFunction( "round", new StandardSQLFunction("round") );
//        registerFunction( "trunc", new StandardSQLFunction("trunc") );
        registerFunction( "ceil", new StandardSQLFunction("ceil") );
        registerFunction( "floor", new StandardSQLFunction("floor") );

        registerFunction( "chr", new StandardSQLFunction("chr", StandardBasicTypes.CHARACTER) );
        registerFunction( "lower", new StandardSQLFunction("lower") );
        registerFunction( "upper", new StandardSQLFunction("upper") );
        registerFunction( "substring", new StandardSQLFunction("substr", StandardBasicTypes.STRING) );
//        registerFunction( "to_ascii", new StandardSQLFunction("to_ascii") );
        registerFunction( "md5", new StandardSQLFunction("md5", StandardBasicTypes.STRING) );
//        registerFunction( "ascii", new StandardSQLFunction("ascii", StandardBasicTypes.INTEGER) );
//        registerFunction( "char_length", new StandardSQLFunction("char_length", StandardBasicTypes.LONG) );
//        registerFunction( "bit_length", new StandardSQLFunction("bit_length", StandardBasicTypes.LONG) );
        registerFunction( "octet_length", new StandardSQLFunction("octet_length", StandardBasicTypes.LONG) );

        registerFunction( "current_date", new NoArgSQLFunction("current_date", StandardBasicTypes.DATE, false) );
        registerFunction( "current_time", new NoArgSQLFunction("current_time", StandardBasicTypes.TIME, false) );
        registerFunction( "current_timestamp", new NoArgSQLFunction("current_timestamp", StandardBasicTypes.TIMESTAMP, false) );
        registerFunction( "date_trunc", new StandardSQLFunction( "date_trunc", StandardBasicTypes.TIMESTAMP ) );
        registerFunction( "now", new NoArgSQLFunction("now", StandardBasicTypes.TIMESTAMP) );
//        registerFunction( "timeofday", new NoArgSQLFunction("timeofday", StandardBasicTypes.STRING) );

//        registerFunction( "current_database", new NoArgSQLFunction("current_database", StandardBasicTypes.STRING, true) );
//        registerFunction( "current_schema", new NoArgSQLFunction("current_schema", StandardBasicTypes.STRING, true) );

        registerFunction( "to_char", new StandardSQLFunction("to_char", StandardBasicTypes.STRING) );
//        registerFunction( "to_date", new StandardSQLFunction("to_date", StandardBasicTypes.DATE) );
//        registerFunction( "to_timestamp", new StandardSQLFunction("to_timestamp", StandardBasicTypes.TIMESTAMP) );
        registerFunction( "to_number", new StandardSQLFunction("to_number", StandardBasicTypes.BIG_DECIMAL) );

        registerFunction( "concat", new VarArgsSQLFunction( StandardBasicTypes.STRING, "(","||",")" ) );

        getDefaultProperties().setProperty( Environment.STATEMENT_BATCH_SIZE, DEFAULT_BATCH_SIZE );
        getDefaultProperties().setProperty( Environment.NON_CONTEXTUAL_LOB_CREATION, "true" );
    }

    @Override
    public SqlTypeDescriptor getSqlTypeDescriptorOverride(int sqlCode) {
        SqlTypeDescriptor descriptor;
        switch ( sqlCode ) {
            case Types.BLOB: {
                // Force BLOB binding.  Otherwise, byte[] fields annotated
                // with @Lob will attempt to use
                // BlobTypeDescriptor.PRIMITIVE_ARRAY_BINDING.  Since the
                // dialect uses oid for Blobs, byte arrays cannot be used.
                descriptor = BlobTypeDescriptor.BLOB_BINDING;
                break;
            }
            case Types.CLOB: {
                descriptor = ClobTypeDescriptor.CLOB_BINDING;
                break;
            }
            default: {
                descriptor = super.getSqlTypeDescriptorOverride( sqlCode );
                break;
            }
        }
        return descriptor;
    }

    @Override
    public String getAddColumnString() {
        return "add column";
    }

//    @Override
//    public String getSequenceNextValString(String sequenceName) {
//        return "select " + getSelectSequenceNextValString( sequenceName );
//    }

//    @Override
//    public String getSelectSequenceNextValString(String sequenceName) {
//        return "nextval ('" + sequenceName + "')";
//    }

//    @Override
//    public String getCreateSequenceString(String sequenceName) {
//        //starts with 1, implicitly
//        return "create sequence " + sequenceName;
//    }

//    @Override
//    public String getDropSequenceString(String sequenceName) {
//        return "drop sequence " + sequenceName;
//    }

//    @Override
//    public String getQuerySequencesString() {
//        return "select * from information_schema.sequences";
//    }

    @Override
    public LimitHandler getLimitHandler() {
        return LIMIT_HANDLER;
    }

    @Override
    public boolean supportsLimit() {
        return true;
    }

    @Override
    public String getLimitString(String sql, boolean hasOffset) {
        return sql + (hasOffset ? " limit ? offset ?" : " limit ?");
    }

    @Override
    public boolean bindLimitParametersInReverseOrder() {
        return true;
    }

//    @Override
//    public String getNativeIdentifierGeneratorStrategy() {
//        return "sequence";
//    }

    @Override
    public boolean supportsOuterJoinForUpdate() {
        return false;
    }

    @Override
    public boolean supportsUnionAll() {
        return true;
    }

    @Override
    public boolean supportsCommentOn() {
        return true;
    }

    @Override
    public boolean supportsIfExistsBeforeTableName() {
        return true;
    }

    @Override
    public MultiTableBulkIdStrategy getDefaultMultiTableBulkIdStrategy() {
        return new LocalTemporaryTableBulkIdStrategy(
                new IdTableSupportStandardImpl() {
                    @Override
                    public String getCreateIdTableCommand() {
                        return "create temporary table";
                    }

                    @Override
                    public String getCreateIdTableStatementOptions() {
                        return "on commit drop";
                    }
                },
                AfterUseAction.CLEAN,
                null
        );
    }

    @Override
    public boolean supportsCurrentTimestampSelection() {
        return true;
    }

    @Override
    public boolean isCurrentTimestampSelectStringCallable() {
        return false;
    }

    @Override
    public String getCurrentTimestampSelectString() {
        return "select now()";
    }

    @Override
    public String toBooleanValueString(boolean bool) {
        return bool ? "true" : "false";
    }

    public ViolatedConstraintNameExtracter getViolatedConstraintNameExtracter() {
        return EXTRACTER;
    }

    /**
     * Constraint-name extractor for Postgres constraint violation exceptions.
     * Originally contributed by Denny Bartelt.
     */
    private static final ViolatedConstraintNameExtracter EXTRACTER = new TemplatedViolatedConstraintNameExtracter() {
        @Override
        protected String doExtractConstraintName(SQLException sqle) throws NumberFormatException {
            final int sqlState = Integer.parseInt( JdbcExceptionHelper.extractSqlState( sqle ) );
            switch (sqlState) {
                // CHECK VIOLATION
                case 23514: return extractUsingTemplate( "violates check constraint \"","\"", sqle.getMessage() );
                // UNIQUE VIOLATION
                case 23505: return extractUsingTemplate( "violates unique constraint \"","\"", sqle.getMessage() );
                // FOREIGN KEY VIOLATION
                case 23503: return extractUsingTemplate( "violates foreign key constraint \"","\"", sqle.getMessage() );
                // NOT NULL VIOLATION
                case 23502: return extractUsingTemplate( "null value in column \"","\" violates not-null constraint", sqle.getMessage() );
                // TODO: RESTRICT VIOLATION
                case 23001: return null;
                // ALL OTHER
                default: return null;
            }
        }
    };

    // Overridden informational metadata ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    @Override
    public boolean supportsEmptyInList() {
        return false;
    }

    @Override
    public boolean supportsLobValueChangePropogation() {
        return false;
    }

    @Override
    public boolean supportsUnboundedLobLocatorMaterialization() {
        return false;
    }

    @Override
    public boolean supportsRowValueConstructorSyntax() {
        return true;
    }

    @Override
    public boolean qualifyIndexName() {
        return false;
    }

    @Override
    public boolean supportsNationalizedTypes() {
        return false;
    }

    @Override
    public boolean supportsJdbcConnectionLobCreation(DatabaseMetaData databaseMetaData) {
        return false;
    }

    @Override
    public boolean supportsValuesList() {
        return true;
    }
}
