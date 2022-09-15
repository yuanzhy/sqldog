package com.yuanzhy.sqldog.jdbc;

import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/12/4
 */
public class DbMetadataTest {

    protected Connection conn;
    @Before
    public void before() throws Exception {
        Class.forName("com.yuanzhy.sqldog.jdbc.Driver");
        conn = DriverManager.getConnection("jdbc:sqldog:mem");
    }

    @Test
    public void test() throws SQLException {
        DatabaseMetaData dbmd = conn.getMetaData();
        System.out.println("------ catalogs ------");
        printRs(dbmd.getCatalogs());
        System.out.println("------ schemas ------");
        printRs(dbmd.getSchemas());
        System.out.println("------ typeinfos ------");
        printRs(dbmd.getTypeInfo());
        System.out.println("------ tables ------");
        printRs(dbmd.getTables("DEFAULT", "sc", "t", null));
        System.out.println("------ columns ------");
        printRs(dbmd.getColumns("DEFAULT", null, null, null));
        System.out.println("------ primaryKeys ------");
        printRs(dbmd.getPrimaryKeys("DEFAULT", null, null));
        System.out.println("------ functions ------");
        printRs(dbmd.getFunctions(null, null, null));
    }

    protected void printRs(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        while (rs.next()) {
            for (int i = 0; i < rsmd.getColumnCount(); i++) {
                final int index = i + 1;
                String columnLabel = rsmd.getColumnLabel(index); // 别名
                Object value = rs.getObject(columnLabel);
                System.out.print(columnLabel + "=" + value);
                if (index != rsmd.getColumnCount()) {
                    System.out.print(", ");
                }
            }
            System.out.println();
        }
    }
}
