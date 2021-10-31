package com.yuanzhy.sqldog.core;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/24
 */
public interface Database extends Base {

    String getEncoding();

    String getDescription();

    String getTablespace();

    Schema getSchema(String name);

    void addSchema(Schema schema);

    void dropSchema(String schemaName);
}
