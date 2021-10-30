package com.yuanzhy.sqldog.calcite;

import java.util.Map;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

/**
 *
 * @author yuanzhy
 * @date 2021-10-29
 */
public class MemorySchemeFactory implements SchemaFactory {

    public static final SchemaFactory INSTANCE = new MemorySchemeFactory();

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        MemorySchema schema = new MemorySchema(name);
        // TODO operand 注入
        return schema;
    }
}
