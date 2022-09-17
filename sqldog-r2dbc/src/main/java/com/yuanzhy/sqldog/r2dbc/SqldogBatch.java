package com.yuanzhy.sqldog.r2dbc;

import java.util.ArrayList;
import java.util.List;

import com.yuanzhy.sqldog.core.util.Asserts;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Result;
import reactor.core.publisher.Flux;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/09/10
 */
final class SqldogBatch implements Batch {

    private final SqldogConnection connection;

    private final String schema;

    private final List<String> statements = new ArrayList<>();

    SqldogBatch(SqldogConnection connection, String schema) {
        this.connection = connection;
        this.schema = schema;
    }

    @Override
    public SqldogBatch add(String sql) {
        Asserts.hasText(sql, "sql must not be null");

        if (!(SqldogSqlParser.parse(sql).getParameterCount() == 0)) {
            throw new IllegalArgumentException(String.format("Statement '%s' is not supported.  This is often due to the presence of parameters.", sql));
        }

        this.statements.add(sql);
        return this;
    }

    @Override
    public Flux<Result> execute() {
        return new SqldogStatement(this.connection, String.join(";", this.statements)).execute();
    }

}
