package com.yuanzhy.sqldog.server.sql.adapter;

import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.util.SourceStringReader;

import java.io.Reader;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/15
 */
public class CalciteParserFactory implements SqlParserImplFactory {

    @Override
    public SqlAbstractParserImpl getParser(Reader reader) {
        SqlAbstractParserImpl parser = new CalciteSqlParser(reader);
        if (reader instanceof SourceStringReader) {
            String sql = ((SourceStringReader) reader).getSourceString();
            parser.setOriginalSql(sql);
        }
        return parser;
    }
}
