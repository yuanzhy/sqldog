package com.yuanzhy.sqldog.server.sql.command.prepared;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.core.util.SqlUtil;
import com.yuanzhy.sqldog.server.sql.PreparedSqlCommand;
import com.yuanzhy.sqldog.server.util.CommandUtil;
import com.yuanzhy.sqldog.server.sql.result.SqlResultBuilder;

import java.io.InputStream;
import java.sql.Blob;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/21
 */
public class SelectPreparedSqlCommand extends AbstractPreparedSqlCommand implements PreparedSqlCommand {

    public SelectPreparedSqlCommand(String preparedSql) {
        super(preparedSql);
    }

    @Override
    public SqlResult execute(Object[] parameter) {
        try {
            ParameterMetaData pmd = ps.getParameterMetaData();
            Asserts.isTrue(pmd.getParameterCount() == parameter.length, "the parameter count mistake");
            for (int i = 1; i <= pmd.getParameterCount(); i++) {
                Object x = parameter[i - 1];
                int type = pmd.getParameterType(i);
                int precision = pmd.getPrecision(i);
//                int scale = pmd.getScale(i);
                boolean nullable = pmd.isNullable(i) == ParameterMetaData.parameterNoNulls;
//                Asserts.isFalse(!nullable && x == null, "the parameter index '"+i+"' must not null");
                if (x == null) {
                    ps.setNull(i, type);
                    continue;
                }
                switch (type) {
                    case Types.CLOB:
                    case Types.DATALINK:
                    case Types.NCLOB:
                    case Types.OTHER:
                    case Types.REF:
                    case Types.SQLXML:
                    case Types.STRUCT:
                        throw SqlUtil.notImplemented();
                    case Types.ARRAY:
                        ps.setArray(i, SqlUtil.toArray(x));
                        break;
                    case Types.BIGINT:
                        ps.setLong(i, SqlUtil.toLong(x));
                        break;
                    case Types.BINARY:
                    case Types.LONGVARBINARY:
                    case Types.VARBINARY:
                        ps.setBytes(i, SqlUtil.toBytes(x));
                        break;
                    case Types.BIT:
                    case Types.BOOLEAN:
                        ps.setBoolean(i, SqlUtil.toBoolean(x));
                        break;
                    case Types.BLOB:
                        if (x instanceof Blob) {
                            ps.setBlob(i, (Blob) x);
                            break;
                        } else if (x instanceof InputStream) {
                            ps.setBlob(i, (InputStream) x);
                        }
                        throw SqlUtil.unsupportedCast(x.getClass(), Blob.class);
                    case Types.DATE:
                        ps.setDate(i, SqlUtil.toDate(x));
                        break;
                    case Types.DECIMAL:
                    case Types.NUMERIC:
                        ps.setBigDecimal(i, SqlUtil.toBigDecimal(x));
                        break;
                    case Types.DISTINCT:
                        throw SqlUtil.notImplemented();
                    case Types.DOUBLE:
                    case Types.FLOAT: // yes really; SQL FLOAT is up to 8 bytes
                        ps.setDouble(i, SqlUtil.toDouble(x));
                        break;
                    case Types.INTEGER:
                        ps.setInt(i, SqlUtil.toInt(x));
                        break;
                    case Types.JAVA_OBJECT:
                        ps.setObject(i, x);
                        break;
                    case Types.LONGNVARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.NVARCHAR:
                    case Types.VARCHAR:
                    case Types.CHAR:
                    case Types.NCHAR:
                        String v = SqlUtil.toString(x);
                        Asserts.isTrue(v.length() <= precision, "Exceeding maximum limit length: " + v.length());
                        ps.setString(i, v);
                        break;
                    case Types.REAL:
                        ps.setFloat(i, SqlUtil.toFloat(x));
                        break;
                    case Types.ROWID:
                        if (x instanceof RowId) {
                            ps.setRowId(i, (RowId) x);
                            break;
                        }
                        throw SqlUtil.unsupportedCast(x.getClass(), RowId.class);
                    case Types.SMALLINT:
                        ps.setShort(i, SqlUtil.toShort(x));
                        break;
                    case Types.TIME:
                        ps.setTime(i, SqlUtil.toTime(x));
                        break;
                    case Types.TIMESTAMP:
                        ps.setTimestamp(i, SqlUtil.toTimestamp(x));
                        break;
                    case Types.TINYINT:
                        ps.setByte(i, SqlUtil.toByte(x));
                        break;
                    default:
                        throw SqlUtil.notImplemented();
                }
            }
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            List<Object[]> data = CommandUtil.resolveResultSet(rs, rsmd);
            return new SqlResultBuilder(StatementType.DQL).columns(rsmd).rows(data.size()).data(data).build();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

//    Map<Integer, Class<?>> sqlTypeClass = new HashMap<>();
//        sqlTypeClass.put(Types.INTEGER, Integer.class);
//        sqlTypeClass.put(Types.BIGINT, Long.class);
//        sqlTypeClass.put(Types.SMALLINT, Short.class);
//        sqlTypeClass.put(Types.TINYINT, Byte.class);
//        sqlTypeClass.put(Types.NUMERIC, BigDecimal.class);
//        sqlTypeClass.put(Types.DECIMAL, BigDecimal.class);
//        sqlTypeClass.put(Types.CHAR, String.class);
//        sqlTypeClass.put(Types.VARCHAR, String.class);
//        sqlTypeClass.put(Types.LONGVARCHAR, String.class);
//        sqlTypeClass.put(Types.DATE, Date.class);
//        sqlTypeClass.put(Types.TIME, Time.class);
//        sqlTypeClass.put(Types.TIMESTAMP, Timestamp.class);
//        sqlTypeClass.put(Types.ARRAY, Array.class);
//        sqlTypeClass.put(Types.BOOLEAN, Boolean.class);
//        sqlTypeClass.put(Types.BINARY, byte[].class);
}
