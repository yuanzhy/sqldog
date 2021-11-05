package com.yuanzhy.sqldog.sql.adapter;

import com.yuanzhy.sqldog.core.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/30
 */
public class CalciteSchema extends AbstractSchema {

    private final Schema schema;

    public CalciteSchema(Schema schema) {
        this.schema = schema;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        Map<String, Table> tableMap = new HashMap<>();
        for (String tableName : schema.getTableNames()) {
            tableMap.put(tableName, new CalciteTable(schema.getTable(tableName)));
        }
        return Collections.unmodifiableMap(tableMap);
    }
}
