package com.yuanzhy.sqldog.server.memory;

import com.yuanzhy.sqldog.server.core.builder.BaseBuilder;
import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.core.util.Asserts;

/**
 *
 * @author yuanzhy
 * @date 2021-10-27
 */
public class SchemaBuilder extends BaseBuilder<SchemaBuilder> {

    @Override
    protected SchemaBuilder getSelf() {
        return this;
    }

    public Schema build() {
        Asserts.hasText(name, "模式名称不能为空");
        return new SchemaMemoryImpl(name);
    }
}