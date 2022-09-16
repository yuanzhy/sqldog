package com.yuanzhy.sqldog.server.sql.parser;

import com.yuanzhy.sqldog.server.sql.PreparedSqlCommand;
import com.yuanzhy.sqldog.server.sql.PreparedSqlParser;
import com.yuanzhy.sqldog.server.sql.command.decorator.SlowLogPreparedCommand;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/9/16
 */
public class SlowLogPreparedSqlParserWrapper implements PreparedSqlParser {

    private PreparedSqlParser sqlParser;
    public SlowLogPreparedSqlParserWrapper(PreparedSqlParser sqlParser) {
        this.sqlParser = sqlParser;
    }
    @Override
    public PreparedSqlCommand parse(String rawSql) {
        return new SlowLogPreparedCommand(sqlParser.parse(rawSql));
    }
}
