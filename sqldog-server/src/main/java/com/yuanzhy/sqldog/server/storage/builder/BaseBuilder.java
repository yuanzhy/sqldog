package com.yuanzhy.sqldog.server.storage.builder;

import com.yuanzhy.sqldog.server.core.Base;

/**
 *
 * @author yuanzhy
 * @date 2021-11-02
 */
public abstract class BaseBuilder<T> {

    protected Base parent;
    protected String name;
    protected String description;

    public T name(String name) {
        this.name = name;
        return getSelf();
    }

    public T description(String description) {
        this.description = description;
        return getSelf();
    }

    public T parent(Base parent) {
        this.parent = parent;
        return getSelf();
    }

    protected abstract T getSelf();

    protected abstract Object build();
}
