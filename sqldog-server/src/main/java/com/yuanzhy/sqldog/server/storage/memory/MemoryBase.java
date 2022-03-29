package com.yuanzhy.sqldog.server.storage.memory;

import com.yuanzhy.sqldog.server.core.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author yuanzhy
 * @date 2021-11-02
 */
public abstract class MemoryBase implements Base {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    protected final String name;
    protected String description = "";

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
        if (description == null) {
            description = "";
        }
        this.description = description;
    }

}
