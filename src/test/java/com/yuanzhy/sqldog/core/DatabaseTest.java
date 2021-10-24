package com.yuanzhy.sqldog.core;

import com.yuanzhy.sqldog.memory.DatabaseMemoryImpl;
import com.yuanzhy.sqldog.memory.SchemaMemoryImpl;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public class DatabaseTest {

    @Test
    public void t() {
        List<DatabaseMemoryImpl> databases = new ArrayList<>();
        databases.add(new DatabaseMemoryImpl("db1"));
        databases.add(new DatabaseMemoryImpl("postgres"));
        databases.add(new DatabaseMemoryImpl("test"));
        SchemaMemoryImpl schema = new SchemaMemoryImpl("test_schema");
//        TableMemoryImpl table = new TableMemoryImpl();
    }
}
