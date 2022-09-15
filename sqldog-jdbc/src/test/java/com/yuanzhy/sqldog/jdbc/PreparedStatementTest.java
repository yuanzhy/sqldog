package com.yuanzhy.sqldog.jdbc;

import com.yuanzhy.sqldog.core.util.DateUtil;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/22
 */
public class PreparedStatementTest extends StatementTest {

    @Test
    public void testDml() throws SQLException {
        conn.setSchema("test");
//        prepareData();
        String sql = "insert into test.tt values(?, ?, ?)";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setInt(1, 4);
        ps.setString(2, "哈哈");
        ps.setDate(3, DateUtil.parseSqlDate("2021-12-11"));
        assert ps.executeUpdate() == 1;

        sql = "update test.tt set name=?,birth=null where id = ?";
        ps = conn.prepareStatement(sql);
        ps.setString(1, "呵呵");
        ps.setInt(2, 4);
        assert ps.executeUpdate() == 1;

        sql = "delete from tt where id=?";
        ps = conn.prepareStatement(sql);
        ps.setInt(1, 1);
        assert ps.executeUpdate() == 1;
    }

    @Test
    public void testDql() throws SQLException {
        conn.setSchema("test");
//        prepareData();
        String sql = "select * from tt where id > ? limit ?";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setInt(1, 1);
        ps.setInt(2, 3);
        ResultSet rs = ps.executeQuery();
        printRs(rs);
    }
    @Test
    public void tt() throws SQLException {
        String sql = "SET SEARCH_PATH TO test;insert into tt values(5, 'ff', '2022-11-12');";
        Statement stat = conn.createStatement();
        stat.execute(sql);
    }
}
