package com.yuanzhy.sqldog.server.storage.memory;

import com.yuanzhy.sqldog.core.util.Asserts;
import com.yuanzhy.sqldog.server.core.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author yuanzhy
 * @date 2021-11-02
 */
public abstract class MemoryBase implements Base {

    protected final transient Logger logger = LoggerFactory.getLogger(this.getClass());
    protected final transient Base parent;
    protected String name;
    protected String description = "";

    protected MemoryBase(Base parent) {
        this.parent = parent;
    }

    protected MemoryBase(Base parent, String name) {
        this.parent = parent;
        this.name = name.toUpperCase();
    }

    @Override
    public String getName() {
        Asserts.hasText(name, "The name is null");
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

    @Override
    public Base getParent() {
        return parent;
    }

    @Override
    public void rename(String newName) {
        Asserts.hasText(newName, "The newName is must not be null");
        this.name = newName.toUpperCase();
    }
}