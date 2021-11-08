package com.yuanzhy.sqldog.server.core;

import com.yuanzhy.sqldog.server.core.constant.ConstraintType;
import com.yuanzhy.sqldog.server.core.constant.DataType;
import com.yuanzhy.sqldog.server.memory.ColumnBuilder;
import com.yuanzhy.sqldog.server.memory.ConstraintBuilder;
import com.yuanzhy.sqldog.server.memory.DatabaseBuilder;
import com.yuanzhy.sqldog.server.memory.SchemaBuilder;
import com.yuanzhy.sqldog.server.memory.TableBuilder;
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
        List<Database> databases = new ArrayList<>();
        databases.add(new DatabaseBuilder().name("db1").build());
        databases.add(new DatabaseBuilder().name("postgres").build());
        databases.add(new DatabaseBuilder().name("test").build());
        Schema schema = new SchemaBuilder().name("test_schema").build();
        databases.get(0).addSchema(schema);
        schema.addTable(new TableBuilder()
                .name("test_table")
                .addColumn(new ColumnBuilder().name("id").dataType(DataType.INT).nullable(false).build())
                .addColumn(new ColumnBuilder().name("name").dataType(DataType.VARCHAR).precision(50).build())
                .addConstraint(new ConstraintBuilder()
                                .type(ConstraintType.PRIMARY_KEY)
                                .addColumnName("id")
                                .build())
                .build()
        );
    }
}