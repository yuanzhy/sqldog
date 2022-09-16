package com.yuanzhy.sqldog.server.sql;

import com.yuanzhy.sqldog.server.common.config.Configs;
import com.yuanzhy.sqldog.server.sql.parser.DefaultSqlParser;
import com.yuanzhy.sqldog.server.sql.parser.DefaultPreparedSqlParser;
import com.yuanzhy.sqldog.server.sql.parser.SlowLogPreparedSqlParserWrapper;
import com.yuanzhy.sqldog.server.sql.parser.SlowLogSqlParserWrapper;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/9/16
 */
public class SqlParserFactory {

    private final static boolean SLOW_LOG = Configs.get().getBoolProperty("sqldog.sql.slowlog");

    public static SqlParser createSqlParser() {
        SqlParser sqlParser = new DefaultSqlParser();
        return SLOW_LOG ? new SlowLogSqlParserWrapper(sqlParser) : sqlParser;
    }

    public static PreparedSqlParser createPreparedSqlParser() {
        PreparedSqlParser sqlParser = new DefaultPreparedSqlParser();
        return SLOW_LOG ? new SlowLogPreparedSqlParserWrapper(sqlParser) : sqlParser;
    }
}
