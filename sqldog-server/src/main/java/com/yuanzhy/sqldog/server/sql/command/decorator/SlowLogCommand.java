package com.yuanzhy.sqldog.server.sql.command.decorator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.common.config.Configs;
import com.yuanzhy.sqldog.server.sql.SqlCommand;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/9/16
 */
public class SlowLogCommand implements SqlCommand {

    protected static final Logger SLOW_LOG = LoggerFactory.getLogger("slowLog");
    protected static final int THRESHOLD = Configs.get().getIntProperty("sqldog.sql.slowlog.threshold", "100");
    protected final SqlCommand delegate;

    public SlowLogCommand(SqlCommand delegate) {
        this.delegate = delegate;
    }

    @Override
    public SqlResult execute() {
        long start = System.currentTimeMillis();
        SqlResult sqlResult = delegate.execute();
        this.log(start);
        return sqlResult;
    }

    @Override
    public void defaultSchema(String schema) {
        delegate.defaultSchema(schema);
    }

    @Override
    public String getSql() {
        return delegate.getSql();
    }

    protected void log(long start) {
        long cost = System.currentTimeMillis() - start;
        if (cost >= THRESHOLD) {
            SLOW_LOG.warn("cost={}, sql={}", cost, getSql());
        } else {
            SLOW_LOG.debug("cost={}, sql={}", cost, getSql());
        }
    }
}
