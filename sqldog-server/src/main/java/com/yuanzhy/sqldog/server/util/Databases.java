package com.yuanzhy.sqldog.server.util;

import com.yuanzhy.sqldog.server.core.Database;
import com.yuanzhy.sqldog.server.memory.DatabaseBuilder;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class Databases {
    // TODO 默认先只支持单实例库
    private static final Database DEFAULT = new DatabaseBuilder().name("default").description("sqldog default db").build();

    public static Database getDefault() {
        return DEFAULT;
    }
}
