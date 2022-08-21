package com.yuanzhy.sqldog.server.core;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.yuanzhy.sqldog.server.util.Calcites;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public class DatabaseTest {

    @Test
    public void t() throws Exception {
        ResultSet rs = Calcites.getConnection().getMetaData().getSchemas();
        ResultSetMetaData rsmd = rs.getMetaData();
        // TODO 返回太多卡死内存占用过大问题
        List<Object[]> data = new ArrayList<>();
        while (rs.next()) {
            Object[] values = new Object[rsmd.getColumnCount()];
            for (int i = 0; i < rsmd.getColumnCount(); i++) {
                final int index = i + 1;
                String columnLabel = rsmd.getColumnLabel(index); // 别名
                String className = rsmd.getColumnClassName(index);
                Object value = null;
                if ("java.sql.Date".equals(className)) {
                    Long date = rs.getLong(columnLabel);
                    if (date != null) {
                        value = new Date(date.longValue());
                    }
                } else if ("java.sql.Time".equals(className)) {
                    Long time = rs.getLong(columnLabel);
                    if (time != null) {
                        value = new Time(time.longValue());
                    }
                } else if ("java.sql.Timestamp".equals(className)) {
                    Long timestamp = rs.getLong(columnLabel);
                    if (timestamp != null) {
                        value = new Timestamp(timestamp.longValue());
                    }
                } else {
                    value = rs.getObject(columnLabel);
                }
                values[i] = value;
            }
            System.out.println(Arrays.toString(values));
            data.add(values);
        }
        System.out.println(data);
    }
}
