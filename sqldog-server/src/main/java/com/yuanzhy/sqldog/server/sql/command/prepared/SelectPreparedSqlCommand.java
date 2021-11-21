package com.yuanzhy.sqldog.server.sql.command.prepared;

import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.sql.PreparedSqlCommand;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/21
 */
public class SelectPreparedSqlCommand extends AbstractPreparedSqlCommand implements PreparedSqlCommand {

    public SelectPreparedSqlCommand(String preparedSql) {
        super(preparedSql);
    }

    @Override
    public SqlResult execute(Object[] parameters) {
        return null;
    }
}
