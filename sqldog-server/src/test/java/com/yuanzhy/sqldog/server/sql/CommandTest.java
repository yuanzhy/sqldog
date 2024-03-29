package com.yuanzhy.sqldog.server.sql;

import org.junit.Test;

import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.sql.parser.DefaultSqlParser;
import com.yuanzhy.sqldog.server.util.Databases;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class CommandTest {

    private SqlParser sqlParser = new DefaultSqlParser();
    @Test
    public void testDDL() {
        sqlParser.parse("create schema ddd").execute();
        sqlParser.parse("create table ddd.abc(id int primary key, name varchar(50) not null)").execute();
        sqlParser.parse("alter table ddd.abc add age int default 10").execute();

        Schema schema = Databases.getDefault().getSchema("ddd");
        assert schema != null;
        assert schema.getTable("abc") != null;
        assert schema.getTable("abc").getColumns().size() == 3;

        sqlParser.parse("drop table ddd.abc").execute();
        assert schema.getTable("abc") == null;

        sqlParser.parse("drop schema ddd").execute();
        assert Databases.getDefault().getSchema("ddd") == null;
    }
}
