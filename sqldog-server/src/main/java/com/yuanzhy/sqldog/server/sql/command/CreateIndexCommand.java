package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.sql.result.SqlResultBuilder;

/**
 *
 * @author yuanzhy
 * @date 2021-11-30
 */
public class CreateIndexCommand extends AbstractSqlCommand {

    public CreateIndexCommand(String sql) {
        super(sql);
    }
    @Override
    public SqlResult execute() {
        // TODO create index
        return new SqlResultBuilder(StatementType.DDL).build();
    }
}
