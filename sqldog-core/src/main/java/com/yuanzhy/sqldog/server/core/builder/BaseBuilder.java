package com.yuanzhy.sqldog.server.core.builder;

/**
 *
 * @author yuanzhy
 * @date 2021-11-02
 */
public abstract class BaseBuilder<T> {

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

    protected abstract T getSelf();

    protected abstract Object build();
}
