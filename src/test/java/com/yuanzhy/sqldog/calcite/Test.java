package com.yuanzhy.sqldog.calcite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Before;

/**
 *
 * @author yuanzhy
 * @date 2021-10-29
 */
public class Test {

    private Connection conn;

    @Before
    public void setup() throws Exception {
        Properties config = new Properties();
        //config.put("model", MyCsvTest.class.getClassLoader().getResource("my_csv_model.json").getPath());
        config.put("caseSensitive", "false");
        conn = DriverManager.getConnection("jdbc:calcite:",config);
    }

    @org.junit.Test
    public void query() throws Exception {
        List<String> sqls = new ArrayList<>();
        sqls.add("select * from csv.csv_user");
        sqls.add("select * from csv.csv_user where id = 1");
        sqls.add("select t1.id, t1.name, t2.age, t2.desc from csv.csv_user t1 left join csv.csv_detail t2 on t1.id = t2.id");
        for (String sql : sqls) {
            System.out.println("执行sql:" + sql);
            printResultSet(conn.createStatement().executeQuery(sql));
        }
    }


    private void printResultSet(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while(resultSet.next()){
            List<Object> row = new ArrayList<>();
            for (int i = 1; i < columnCount+1; i++) {
                row.add(resultSet.getObject(i));
            }
            System.out.println(row);
        }
    }
}
