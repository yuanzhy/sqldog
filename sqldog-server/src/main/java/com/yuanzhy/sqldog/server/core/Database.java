package com.yuanzhy.sqldog.server.core;

import java.util.Set;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public interface Database extends Base {

    String getEncoding();

    String getTablespace();

    Set<String> getSchemaNames();

    Schema getSchema(String name);

    void addSchema(Schema schema);

    void dropSchema(String schemaName);
}
