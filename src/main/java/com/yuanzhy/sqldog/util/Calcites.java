package com.yuanzhy.sqldog.util;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.util.ConversionUtil;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class Calcites {

    public static final SchemaPlus SCHEMA_PLUS = Frameworks.createRootSchema(true);
    private static final FrameworkConfig FRAMEWORK_CONFIG;
    static {
        FRAMEWORK_CONFIG = Frameworks.newConfigBuilder()
                .defaultSchema(SCHEMA_PLUS)
                .parserConfig(SqlParser.config()
                                .withParserFactory(SqlParserImpl.FACTORY)
                                .withCaseSensitive(false)
                        //.withQuoting(Quoting.BACK_TICK)
                        //.withQuotedCasing(Casing.TO_UPPER)
                        //.withUnquotedCasing(Casing.TO_UPPER)
                        //.withConformance(SqlConformanceEnum.ORACLE_12)
                ).build();
        System.setProperty("saffron.default.charset", ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
        System.setProperty("saffron.default.nationalcharset",ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
        System.setProperty("saffron.default.collation.name",ConversionUtil.NATIVE_UTF16_CHARSET_NAME + "$en_US");
    }

    public static Planner getPanner() {
        return Frameworks.getPlanner(FRAMEWORK_CONFIG);
    }
}
