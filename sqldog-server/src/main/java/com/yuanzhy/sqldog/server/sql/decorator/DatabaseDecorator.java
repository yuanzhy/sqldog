package com.yuanzhy.sqldog.server.sql.decorator;

import java.util.Set;

import com.yuanzhy.sqldog.server.core.Database;
import com.yuanzhy.sqldog.server.core.Schema;
import com.yuanzhy.sqldog.server.sql.adapter.CalciteSchema;
import com.yuanzhy.sqldog.server.util.Calcites;

/**
 *
 * @author yuanzhy
 * @date 2021-11-15
 */
public class DatabaseDecorator implements Database {

    private final Database delegate;
    public DatabaseDecorator(Database delegate) {
        this.delegate = delegate;
    }
    @Override
    public String getEncoding() {
        return delegate.getEncoding();
    }

    @Override
    public String getTablespace() {
        return delegate.getTablespace();
    }

    @Override
    public Set<String> getSchemaNames() {
        return delegate.getSchemaNames();
    }

    @Override
    public Schema getSchema(String name) {
        return delegate.getSchema(name);
    }

    @Override
    public void addSchema(Schema schema) {
        delegate.addSchema(schema);
        Calcites.getRootSchema().add(schema.getName(), new CalciteSchema(schema));
    }

    @Override
    public void dropSchema(String schemaName) {
        delegate.dropSchema(schemaName);
        org.apache.calcite.jdbc.CalciteSchema calciteSchema = Calcites.getRootSchema().unwrap(org.apache.calcite.jdbc.CalciteSchema.class);
        calciteSchema.removeSubSchema(schemaName);
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public String getDescription() {
        return delegate.getDescription();
    }

    @Override
    public void setDescription(String description) {
        delegate.setDescription(description);
    }

    @Override
    public String toPrettyString() {
        return delegate.toPrettyString();
    }

    @Override
    public void drop() {
        org.apache.calcite.jdbc.CalciteSchema calciteSchema = Calcites.getRootSchema().unwrap(org.apache.calcite.jdbc.CalciteSchema.class);
        for (String schemaName : delegate.getSchemaNames()) {
            calciteSchema.removeSubSchema(schemaName);
        }
        delegate.drop();
    }
}
