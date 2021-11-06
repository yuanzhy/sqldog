package com.yuanzhy.sqldog.calcite;

import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.core.constant.ConstraintType;
import com.yuanzhy.sqldog.server.core.constant.DataType;
import com.yuanzhy.sqldog.server.memory.ColumnBuilder;
import com.yuanzhy.sqldog.server.memory.ConstraintBuilder;
import com.yuanzhy.sqldog.server.memory.SchemaBuilder;
import com.yuanzhy.sqldog.server.memory.TableBuilder;
import com.yuanzhy.sqldog.server.sql.adapter.CalciteSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.util.ConversionUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/30
 */
public class TestQuery {
    private Connection conn;
    @Before
    public void setUp() throws SQLException {
        Table table = new TableBuilder().name("PERSON")
                .addColumn(new ColumnBuilder().name("ID").dataType(DataType.INT).nullable(false).build())
                .addColumn(new ColumnBuilder().name("NAME").dataType(DataType.VARCHAR).precision(50).build())
                .addColumn(new ColumnBuilder().name("AGE").dataType(DataType.INT).build())
                .addConstraint(new ConstraintBuilder().type(ConstraintType.PRIMARY_KEY).addColumnName("ID").build())
                .build();
        Schema schema = new SchemaBuilder().name("TEST").build();
        schema.addTable(table);

        Map<String, Object> values = new HashMap<>();
        values.put("ID", 1);
        values.put("NAME", "张三");
        values.put("AGE", 10);
        table.getDML().insert(values);
        values = new HashMap<>();
        values.put("ID", 2);
        values.put("NAME", "李四");
        values.put("AGE", 15);
        table.getDML().insert(values);
        values = new HashMap<>();
        values.put("ID", 3);
        values.put("NAME", "王五");
        values.put("AGE", 15);
        table.getDML().insert(values);
        System.setProperty("saffron.default.charset", ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
        System.setProperty("saffron.default.nationalcharset",ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
        System.setProperty("saffron.default.collation.name",ConversionUtil.NATIVE_UTF16_CHARSET_NAME + "$en_US");
        Properties config = new Properties();
        //config.put("model", MyCsvTest.class.getClassLoader().getResource("my_csv_model.json").getPath());
        config.put("caseSensitive", "false");
        conn = DriverManager.getConnection("jdbc:calcite:", config);

        CalciteConnection calciteConnection = conn.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        rootSchema.add("TEST", new CalciteSchema(schema));
    }

//    @Test
//    public void insert() throws Exception {
//        List<String> sqls = new ArrayList<>();
//        sqls.add("insert into test.person values(1, '张三', 15)");
//        sqls.add("insert into test.person values(2, '李四', 20)");
//        sqls.add("insert into test.person values(3, '王五', 20)");
//        for (String sql : sqls) {
//            System.out.println("insert sql:" + sql);
//            conn.createStatement().execute(sql);
//        }
//
//        query();
//    }

    @Test
    public void query() throws Exception {
        List<String> sqls = new ArrayList<>();
        sqls.add("select * from test.person");
        sqls.add("select count(*) from test.person");
        sqls.add("select age, count(*) from test.person group by age");
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

    @After
    public void close() throws SQLException {
        conn.close();
    }
}
