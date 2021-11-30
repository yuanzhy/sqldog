package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.sql.result.SqlResultBuilder;

/**
 *
 * @author yuanzhy
 * @date 2021-11-30
 */
public class DropIndexCommand extends AbstractSqlCommand {
    public DropIndexCommand(String sql) {
        super(sql);
    }

    @Override
    public SqlResult execute() {
        // drop index i_xxx
        //String sqlSuffix = sql.substring("DROP INDEX ".length());
        // TODO index
        //if (sqlSuffix.startsWith("IF EXISTS ")) {
        //
        //} else {
        //
        //}
        return new SqlResultBuilder(StatementType.DDL).build();
    }
}