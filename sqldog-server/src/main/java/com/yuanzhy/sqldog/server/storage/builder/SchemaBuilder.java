package com.yuanzhy.sqldog.server.storage.builder;

import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.server.common.config.Configs;
import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.storage.disk.DiskSchema;
import com.yuanzhy.sqldog.server.storage.memory.MemorySchema;

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
        Asserts.notNull(parent, "模式parent不能为空");
        return Configs.get().isDisk()
                ? new DiskSchema(parent, name, description)
                : new MemorySchema(parent, name, description);
    }
}
