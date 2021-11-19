package com.yuanzhy.sqldog.server.sql.adapter;

import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.util.SourceStringReader;
import org.apache.commons.lang3.StringUtils;

import java.io.Reader;

import com.yuanzhy.sqldog.server.util.Databases;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/15
 */
public class CalciteParserFactory implements SqlParserImplFactory {

    @Override
    public SqlAbstractParserImpl getParser(Reader reader) {
        String schema = Databases.currSchema();
        SqlAbstractParserImpl parser;
        if (StringUtils.isEmpty(schema)) {
            parser = new SqlParserImpl(reader);
        } else {
            parser = new CalciteSqlParser(reader, schema);
        }
        if (reader instanceof SourceStringReader) {
            final String sql = ((SourceStringReader) reader).getSourceString();
            parser.setOriginalSql(sql);
        }
        return parser;
    }
}
