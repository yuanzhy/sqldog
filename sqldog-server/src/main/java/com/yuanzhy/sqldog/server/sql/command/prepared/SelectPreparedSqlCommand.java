package com.yuanzhy.sqldog.server.sql.command.prepared;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.sql.PreparedSqlCommand;
import com.yuanzhy.sqldog.server.sql.result.SqlResultBuilder;

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
        Map<Integer, Class<?>> sqlTypeClass = new HashMap<>();
        sqlTypeClass.put(Types.INTEGER, Integer.class);
        sqlTypeClass.put(Types.BIGINT, Long.class);
        sqlTypeClass.put(Types.SMALLINT, Short.class);
        sqlTypeClass.put(Types.TINYINT, Byte.class);
        sqlTypeClass.put(Types.NUMERIC, BigDecimal.class);
        sqlTypeClass.put(Types.DECIMAL, BigDecimal.class);
        sqlTypeClass.put(Types.CHAR, String.class);
        sqlTypeClass.put(Types.VARCHAR, String.class);
        sqlTypeClass.put(Types.LONGVARCHAR, String.class);
        sqlTypeClass.put(Types.DATE, Date.class);
        sqlTypeClass.put(Types.TIME, Time.class);
        sqlTypeClass.put(Types.TIMESTAMP, Timestamp.class);
        sqlTypeClass.put(Types.ARRAY, Array.class);
        sqlTypeClass.put(Types.BOOLEAN, Boolean.class);
        sqlTypeClass.put(Types.BINARY, byte[].class);
        try {
            // TODO setParameter
            ParameterMetaData pmd = ps.getParameterMetaData();
            if (pmd.getParameterCount() != parameter.length) {
                throw new IllegalArgumentException("the parameter count mistake");
            }
            for (int i = 1; i <= pmd.getParameterCount(); i++) {
                Object param = parameter[i - 1];
                Object p1 = pmd.getParameterMode(i);
                Object p2 = pmd.getParameterClassName(i);
                int type = pmd.getParameterType(i);
                Object p4 = pmd.getParameterTypeName(i);
                Object p5 = pmd.getPrecision(i);
                Object p6 = pmd.getScale(i);
                boolean nullable = pmd.isNullable(i) == ParameterMetaData.parameterNoNulls;
                Object p8 = pmd.isSigned(i);
                Class<?> cls = sqlTypeClass.get(type);
                if (!nullable && param == null) {
                    throw new IllegalArgumentException("the parameter index '"+i+"' must not null");
                }
                if (!cls.isInstance(param)) {
                    // TODO
                    throw new IllegalArgumentException("");
                }
                System.out.println(p1);
            }
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            // TODO 返回太多卡死内存占用过大问题
            List<Object[]> data = new ArrayList<>();
            while (rs.next()) {
                Object[] values = new Object[rsmd.getColumnCount()];
                for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    Object value = rs.getObject(rsmd.getColumnLabel(i+1));
                    values[i] = value;
                }
                data.add(values);
            }
            return new SqlResultBuilder(StatementType.DQL).columns(rsmd).rows(data.size()).data(data).build();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
