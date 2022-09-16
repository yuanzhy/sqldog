package com.yuanzhy.sqldog.server.sql.command.decorator;

import java.io.IOException;
import java.util.Arrays;

import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.sql.PreparedSqlCommand;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/9/16
 */
public class SlowLogPreparedCommand extends SlowLogCommand implements PreparedSqlCommand {

    public SlowLogPreparedCommand(PreparedSqlCommand delegate) {
        super(delegate);
    }

    protected PreparedSqlCommand getDelegate() {
        return (PreparedSqlCommand) delegate;
    }

    @Override
    public SqlResult execute(Object[] parameter) {
        long start = System.currentTimeMillis();
        SqlResult sqlResult = getDelegate().execute(parameter);
        this.log(start, parameter);
        return sqlResult;
    }

    @Override
    public void close() throws IOException {
        getDelegate().close();
    }

    protected void log(long start, Object[] parameter) {
        long cost = System.currentTimeMillis() - start;
        if (cost >= THRESHOLD) {
            SLOW_LOG.warn("cost={}, sql={}, param={}", cost, getSql(), Arrays.toString(parameter));
        } else {
            SLOW_LOG.debug("cost={}, sql={}, param={}", cost, getSql(), Arrays.toString(parameter));
        }
    }
}
