package com.yuanzhy.sqldog.server.sql.adapter;

import com.yuanzhy.sqldog.server.core.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/30
 */
public class CalciteSchema extends AbstractSchema implements Observer {

    private Map<String, Table> tableMap;
    private final Schema schema;

    public CalciteSchema(Schema schema) {
        this.schema = schema;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        if (this.tableMap == null) {
            this.loadTables();
        }
        return this.tableMap;
    }

    @Override
    public void update(Observable o, Object arg) {
        if (this.tableMap != null) {
            this.loadTables();
        }
    }

    private void loadTables() {
        Map<String, Table> tableMap = new HashMap<>();
        for (String tableName : schema.getTableNames()) {
//            tableMap.put(tableName, new ScannableCalciteTable(schema.getTable(tableName)));
            tableMap.put(tableName, new FilterableCalciteTable(schema.getTable(tableName)));
//            tableMap.put(tableName, new TranslatableCalciteTable(schema.getTable(tableName)));
        }
        this.tableMap = Collections.unmodifiableMap(tableMap);
    }
}
