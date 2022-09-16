package com.yuanzhy.sqldog.server.sql.parser;

import com.yuanzhy.sqldog.server.sql.SqlCommand;
import com.yuanzhy.sqldog.server.sql.SqlParser;
import com.yuanzhy.sqldog.server.sql.command.decorator.SlowLogCommand;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/9/16
 */
public class SlowLogSqlParserWrapper implements SqlParser {

    private SqlParser sqlParser;
    public SlowLogSqlParserWrapper(SqlParser sqlParser) {
        this.sqlParser = sqlParser;
    }
    @Override
    public SqlCommand parse(String rawSql) {
        return new SlowLogCommand(sqlParser.parse(rawSql));
    }
}
