package com.yuanzhy.sqldog.server.sql.command.prepared;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.ParamMetaData;
import com.yuanzhy.sqldog.core.sql.ParamMetaDataImpl;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.core.util.DateUtil;
import com.yuanzhy.sqldog.core.util.SqlUtil;
import com.yuanzhy.sqldog.server.core.Column;
import com.yuanzhy.sqldog.server.sql.PreparedSqlCommand;
import com.yuanzhy.sqldog.server.sql.command.AbstractSqlCommand;
import com.yuanzhy.sqldog.server.sql.result.SqlResultBuilder;
import com.yuanzhy.sqldog.server.util.Calcites;
import com.yuanzhy.sqldog.server.util.Databases;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import java.io.IOException;
import java.sql.ParameterMetaData;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/22
 */
public class UpdatePreparedSqlCommand extends AbstractSqlCommand implements PreparedSqlCommand {
    private final SqlUpdate sqlUpdate;
    //private final ColumnMetaData[] columns;
    private final ParamMetaData[] params;
    public UpdatePreparedSqlCommand(String preparedSql) {
        super(preparedSql);
        try {
            sqlUpdate = (SqlUpdate) Calcites.getPanner().parse(preparedSql);
            super.parseSchemaTable(sqlUpdate.getTargetTable().toString());
            SqlNodeList targetColumnList = sqlUpdate.getTargetColumnList();
            //int size = targetColumnList == null ? 0 : targetColumnList.size();
            // handle rsmd
            //this.columns = new ColumnMetaData[size];
            //for (int i = 0; i < size; i++) {
            //    SqlNode colNode = targetColumnList.get(i);
            //    Column column = findColumn(colNode);
            //    columns[i] = new ColumnMetaDataBuilder().label(column.getName())
            //            .columnName(column.getName()).ordinal(i)
            //            .autoIncrement(column.getDataType().isSerial())
            //            .catalogName(Databases.getDefault().getName()).schemaName(schema.getName())
            //            .tableName(table.getName()).caseSensitive(false)
            //            .columnClassName(column.getDataType().getClazz().getName())
            //            .displaySize(column.getPrecision()).nullable(column.isNullable() ?
            //                    ResultSetMetaData.columnNullable :
            //                    ResultSetMetaData.columnNoNulls).scale(column.getScale())
            //            .precision(column.getPrecision()).searchable(true)
            //            .columnType(column.getDataType().getSqlType())
            //            .columnTypeName(column.getDataType().name()).build();
            //}
            // handle pmd
            List<String> colNames = new ArrayList<>();
            SqlNodeList snl = sqlUpdate.getSourceExpressionList();
            if (snl != null) {
                for (int i = 0; i < snl.size(); i++) {
                    if (snl.get(i) instanceof SqlDynamicParam) {
                        SqlNode colNode = targetColumnList.get(i);
                        Column column = findColumn(colNode);
                        colNames.add(column.getName());
                    }
                }
            }
            SqlNode condNode = sqlUpdate.getCondition();
            if (condNode != null && condNode instanceof SqlBasicCall) {
                collectColName((SqlBasicCall) condNode, colNames);
            }
            this.params = new ParamMetaData[colNames.size()];
            for (int i = 0; i < colNames.size(); i++) {
                Column column = table.getColumn(colNames.get(i));
                Asserts.notNull(column, "column not found: " + colNames);
                params[i] = new ParamMetaDataImpl(
                        false,
                        column.getPrecision(),
                        column.getScale(),
                        column.getDataType().getSqlType(),
                        column.getDataType().name(),
                        column.getDataType().getClazz().getName(),
                        ParameterMetaData.parameterModeIn,
                        column.isNullable() ? ParameterMetaData.parameterNullable : ParameterMetaData.parameterNoNulls
                );
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SqlResult execute(Object[] parameter) {
        Asserts.notNull(parameter, "parameter can not null");
        Asserts.isTrue(parameter.length == params.length, "parameter count mistake: " + parameter.length + " != " + params.length);
        try {
            SqlUpdate sqlUpdate = (SqlUpdate) Calcites.getPanner().parse(sql);
            replacePlaceHolder(sqlUpdate, parameter, new AtomicInteger(0));
            int rows = table.getTableData().updateBy(sqlUpdate);
            return new SqlResultBuilder(StatementType.DML).schema(schema.getName()).table(table.getName()).rows(rows).build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SqlResult execute() {
        return new SqlResultBuilder(StatementType.DML).schema(schema.getName()).table(table.getName())
                /*.columns(columns)*/.params(params).build();
    }

    private Column findColumn(SqlNode sqlNode) {
        if (sqlNode.getKind() != SqlKind.IDENTIFIER) {
            throw new UnsupportedOperationException("not supported: " + sqlNode.toString());
        }
        SqlIdentifier colIdfy = (SqlIdentifier) sqlNode;
        String colName = colIdfy.names.get(colIdfy.names.size() - 1);
        Column column = table.getColumn(colName);
        Asserts.notNull(column, "column not found: " + colIdfy.toString());
        return column;
    }

    private void collectColName(SqlBasicCall where, List<String> colNames) {
        for (SqlNode sqlNode : where.getOperandList()) {
            if (sqlNode instanceof SqlBasicCall) {
                collectColName((SqlBasicCall) sqlNode, colNames);
            }
        }
        boolean containsPlaceholder = where.getOperandList().stream().filter(sn -> sn instanceof SqlDynamicParam).findFirst().isPresent();
        if (containsPlaceholder) {
            where.getOperandList().stream().filter(sn -> sn instanceof SqlIdentifier).findFirst().ifPresent(sqlIdty -> {
                String colName = ((SqlIdentifier)sqlIdty).names.get(((SqlIdentifier)sqlIdty).names.size() - 1);
                colNames.add(colName);
            });
        }
    }

    private void replacePlaceHolder(SqlBasicCall basicCall, Object[] parameter, AtomicInteger index)
            throws SQLFeatureNotSupportedException {
        List<SqlNode> opList = basicCall.getOperandList();
        for (int i = 0; i < opList.size(); i++) {
            SqlNode sqlNode = opList.get(i);
            if (sqlNode instanceof SqlBasicCall) {
                replacePlaceHolder((SqlBasicCall) sqlNode, parameter, index);
            } else if (sqlNode instanceof SqlDynamicParam) {
                Optional<SqlNode> op = basicCall.getOperandList().stream().filter(sn -> sn instanceof SqlIdentifier).findFirst();
                if (op.isPresent()) {
                    Column column = findColumn(op.get());
                    basicCall.setOperand(i, toTypedNode(column, parameter[index.getAndIncrement()]));
                }
            }
        }
    }

    private void replacePlaceHolder(SqlUpdate sqlUpdate, Object[] parameter, AtomicInteger index)
            throws SQLFeatureNotSupportedException {
        SqlNodeList snl = sqlUpdate.getSourceExpressionList();
        SqlNodeList tcl = sqlUpdate.getTargetColumnList();
        if (snl != null) {
            for (int i = 0; i < snl.size(); i++) {
                if (snl.get(i) instanceof SqlDynamicParam) {
                    SqlNode colNode = tcl.get(i);
                    Column column = findColumn(colNode);
                    Object param = parameter[index.getAndIncrement()];
                    SqlNode node = toTypedNode(column, param);
                    snl.set(i, node);
                }
            }
        }
        SqlNode condNode = sqlUpdate.getCondition();
        if (condNode != null && condNode instanceof SqlBasicCall) {
            replacePlaceHolder((SqlBasicCall) condNode, parameter, index);
        }
    }

    private SqlNode toTypedNode(Column column, Object x) throws SQLFeatureNotSupportedException {
        SqlParserPos pos = SqlParserPos.ZERO;
        if (x == null) {
            return SqlLiteral.createNull(pos);
        }
        switch (column.getDataType()) {
            case INT:
            case SERIAL:
            case BIGINT:
            case BIGSERIAL:
            case SMALLSERIAL:
            case SMALLINT:
            case TINYINT:
            case DECIMAL:
            case NUMERIC:
            case FLOAT:
            case DOUBLE:
                return SqlLiteral.createExactNumeric(x.toString(), pos);
            case DATE:
                Calendar cal = Calendar.getInstance();
                cal.setTime(SqlUtil.toDate(x));
                return SqlLiteral.createDate(cal, pos);
            case TIME:
                TimeString ts = new TimeString(DateUtil.formatTime(SqlUtil.toTime(x)));
                return SqlLiteral.createTime(ts, 0, pos);
            case TIMESTAMP:
                TimestampString tts = new TimestampString(DateUtil.formatTimestamp(SqlUtil.toTimestamp(x)));
                return SqlLiteral.createTimestamp(tts, 0, pos);
            case CHAR:
            case VARCHAR:
            case TEXT:
                return SqlLiteral.createCharString(SqlUtil.toString(x), "UTF-8", pos);
            case BOOLEAN:
                return SqlLiteral.createBoolean(SqlUtil.toBoolean(x), pos);
            case BYTEA:
                return SqlLiteral.createBinaryString(SqlUtil.toBytes(x), pos);
            //case ARRAY: // TODO
            //    SqlLiteral.createSqlCharStringLiteral(SqlUtil.toString(x), pos);
            //case JSON:
            //    return x;
            default:
                throw SqlUtil.notImplemented();
        }
    }

    @Override
    public void currentSchema(String schema) {
        Databases.currSchema(schema);
    }

    @Override
    public void close() throws IOException {
        // noop
    }
}
