package com.yuanzhy.sqldog.memory;

import com.yuanzhy.sqldog.builder.BaseBuilder;
import com.yuanzhy.sqldog.core.Database;
import com.yuanzhy.sqldog.core.util.Asserts;

/**
 *
 * @author yuanzhy
 * @date 2021-10-27
 */
public class DatabaseBuilder extends BaseBuilder<DatabaseBuilder> {

    /** 名称 */
    private String name;
    /** 编码 */
    private String encoding = "UTF-8";
    /** 描述 */
    private String description;
    /** 表空间 */
    private String tablespace;

    public DatabaseBuilder name(String name) {
        this.name = name;
        return this;
    }

    public DatabaseBuilder encoding(String encoding) {
        this.encoding = encoding;
        return this;
    }

    public DatabaseBuilder description(String description) {
        this.description = description;
        return this;
    }

    @Override
    protected DatabaseBuilder getSelf() {
        return this;
    }

    public DatabaseBuilder tablespace(String tablespace) {
        this.tablespace = tablespace;
        return this;
    }

    @Override
    public Database build() {
        Asserts.hasText(name, "数据库名称不能为空");
        Asserts.hasText(encoding, "数据库编码不能为空");
        return new DatabaseMemoryImpl(name, encoding, description, tablespace);
    }
}
