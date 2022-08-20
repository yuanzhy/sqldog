//package com.yuanzhy.sqldog.jdbc;
//
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.sql.Statement;
//
///**
// * @author yuanzhy
// * @version 1.0
// * @date 2021/11/21
// */
//public class StatementTest {
//
//    protected Connection conn;
//    protected Statement stat;
//    @Before
//    public void before() throws Exception {
//        Class.forName("com.yuanzhy.sqldog.jdbc.Driver");
//        conn = DriverManager.getConnection("jdbc:sqldog://127.0.0.1:2345", "root", "123456");
//        stat = conn.createStatement();
//        try {
//            stat.execute("create schema test");
//        } catch (Exception e) {
//            // ignore
//        }
//    }
//
//    protected void prepareData() throws SQLException {
//        ResultSet rs;
//        stat.execute("create table test.tt (id int primary key, name varchar(20))");
//
//        stat.execute("insert into test.tt values(1, 'z(s)')");
//        rs = stat.getResultSet();
//        rs.next();
//        assert rs.getInt(1) == 1;
//
//        stat.execute("insert into test.tt values(2, '李四')");
//        rs = stat.getResultSet();
//        rs.next();
//        assert rs.getInt(1) == 2;
//
//        stat.execute("insert into test.tt values(3, null)");
//
//        stat.execute("alter table test.tt add birth date");
//        int c = stat.executeUpdate("update test.tt set birth = '2021-11-21'");
//        assert c == 3;
//
//    }
//
//    @Test
//    public void exec() throws SQLException {
//        conn.setSchema("test");
//        prepareData();
//        ResultSet rs = stat.executeQuery("select * from tt");
//        printRs(rs);
//    }
//
//    protected void printRs(ResultSet rs) throws SQLException {
//        System.out.println("---------- iterate by label");
//        while (rs.next()) {
//            String s = String.format("id: %s, name: %s, birth: %s", rs.getInt("id"), rs.getString("name"), rs.getDate("birth"));
//            System.out.println(s);
//        }
//        rs.beforeFirst();
//        System.out.println("---------- iterate by index");
//        while (rs.next()) {
//            String s = String.format("id: %s, name: %s, birth: %s", rs.getInt(1), rs.getString(2), rs.getDate(3));
//            System.out.println(s);
//        }
//    }
//
//    @After
//    public void after() throws SQLException {
////        try {
////            stat.execute("drop table tt");
////        } catch (Exception e) {
////
////        }
//        conn.close();
//    }
//}
