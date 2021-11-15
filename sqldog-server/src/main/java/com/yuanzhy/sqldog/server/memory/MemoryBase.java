package com.yuanzhy.sqldog.server.memory;

import com.yuanzhy.sqldog.server.core.Base;
import com.yuanzhy.sqldog.server.util.FormatterUtil;

/**
 *
 * @author yuanzhy
 * @date 2021-11-02
 */
public abstract class MemoryBase implements Base {

    protected final String name;
    protected String description;

    protected MemoryBase(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public void setDescription(String description) {
        this.description = description;
    }

    protected String joinByVLine(String... values) {
        return FormatterUtil.joinByVLine(15, values);
    }

    protected String genHLine(int count) {
        return FormatterUtil.genHLine(15, count);
    }
}
