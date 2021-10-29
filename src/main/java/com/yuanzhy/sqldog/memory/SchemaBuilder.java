package com.yuanzhy.sqldog.memory;

import com.yuanzhy.sqldog.core.Schema;
import com.yuanzhy.sqldog.util.Asserts;

/**
 *
 * @author yuanzhy
 * @date 2021-10-27
 */
public class SchemaBuilder {

    /** 名称 */
    private String name;

    public SchemaBuilder name(String name) {
        this.name = name;
        return this;
    }

    public Schema build() {
        Asserts.hasText(name, "模式名称不能为空");
        return new SchemaMemoryImpl(name);
    }
}
