package com.yuanzhy.sqldog.memory;

import com.yuanzhy.sqldog.core.Base;

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
}
