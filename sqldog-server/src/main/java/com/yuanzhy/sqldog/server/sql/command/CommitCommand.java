package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.sql.SqlCommand;
import com.yuanzhy.sqldog.server.sql.result.SqlResultBuilder;

/**
 *
 * @author yuanzhy
 * @date 2021-11-23
 */
public class CommitCommand extends AbstractSqlCommand implements SqlCommand {

    public CommitCommand(String sql) {
        super(sql);
    }

    @Override
    public SqlResult execute() {
        // TODO
        return new SqlResultBuilder(StatementType.DTL).build();
    }
}
