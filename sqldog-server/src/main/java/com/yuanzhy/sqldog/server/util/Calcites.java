package com.yuanzhy.sqldog.server.util;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.util.ConversionUtil;

import com.yuanzhy.sqldog.core.constant.Consts;
import com.yuanzhy.sqldog.server.sql.adapter.CalciteParserFactory;
import com.yuanzhy.sqldog.server.sql.adapter.sys.ColumnSysTable;
import com.yuanzhy.sqldog.server.sql.adapter.sys.PrimaryKeySysTable;
import com.yuanzhy.sqldog.server.sql.adapter.sys.SchemaSysTable;
import com.yuanzhy.sqldog.server.sql.adapter.sys.TableSysTable;
import com.yuanzhy.sqldog.server.sql.function.ScalarFunctions;
import com.yuanzhy.sqldog.server.sql.function.agg.StringAggFunction;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class Calcites {

    private static final SchemaPlus SCHEMA_PLUS /*= Frameworks.createRootSchema(true)*/;
    private static final FrameworkConfig FRAMEWORK_CONFIG;
    private static final Planner PLANNER;
    private static final CalciteConnection CONNECTION;
    static {
//        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
//        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        System.setProperty("saffron.default.charset", ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
        System.setProperty("saffron.default.nationalcharset",ConversionUtil.NATIVE_UTF16_CHARSET_NAME);
        System.setProperty("saffron.default.collation.name",ConversionUtil.NATIVE_UTF16_CHARSET_NAME + "$en_US");
        System.setProperty("calcite.bindable.cache.maxSize", "200");
        Properties config = new Properties();
        //config.put("model", MyCsvTest.class.getClassLoader().getResource("my_csv_model.json").getPath());
        config.put("parserFactory", "com.yuanzhy.sqldog.server.sql.adapter.CalciteParserFactory");
        config.put("caseSensitive", "false");
//        config.put("conformance", "STRICT_2003");
//        config.put("timeZone", "UTC");
        try {
            Connection conn = DriverManager.getConnection("jdbc:calcite:", config);
            CONNECTION = conn.unwrap(CalciteConnection.class);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        SCHEMA_PLUS = CONNECTION.getRootSchema();
        registerFn();
        registerSysTable();

        FRAMEWORK_CONFIG = Frameworks.newConfigBuilder()
                .defaultSchema(SCHEMA_PLUS)
                .parserConfig(SqlParser.config()
                                .withParserFactory(new CalciteParserFactory())
                                .withCaseSensitive(false)
                        //.withQuoting(Quoting.BACK_TICK)
                        //.withQuotedCasing(Casing.TO_UPPER)
                        //.withUnquotedCasing(Casing.TO_UPPER)
//                        .withConformance(SqlConformanceEnum.STRICT_2003)
                )
//                .operatorTable(sqlStdOperatorTable)
                .build();
        PLANNER = Frameworks.getPlanner(FRAMEWORK_CONFIG);
    }

    public static Planner getPanner() {
        return PLANNER;
    }

    public static Connection getConnection() {
        return CONNECTION;
    }

    public static SchemaPlus getRootSchema() {
        return SCHEMA_PLUS;
    }

    private static void registerFn() {
        // 1. scalar function
        Method[] methods = ScalarFunctions.class.getDeclaredMethods();
        for (Method method : methods) {
            String name = method.getName();
            if (Modifier.isPrivate(method.getModifiers())) {
                continue;
            }
            SCHEMA_PLUS.add(name.toUpperCase(), ScalarFunctionImpl.create(ScalarFunctions.class, name));
        }
        // 2. agg function
        SCHEMA_PLUS.add("STRING_AGG", AggregateFunctionImpl.create(StringAggFunction.class));
    }

    private static void registerSysTable() {
        SCHEMA_PLUS.add(Consts.SYSTABLE_PREFIX.concat("SCHEMA"), new SchemaSysTable());
        SCHEMA_PLUS.add(Consts.SYSTABLE_PREFIX.concat("TABLE"), new TableSysTable());
        SCHEMA_PLUS.add(Consts.SYSTABLE_PREFIX.concat("COLUMN"), new ColumnSysTable());
        SCHEMA_PLUS.add(Consts.SYSTABLE_PREFIX.concat("PRIMARYKEY"), new PrimaryKeySysTable());
    }
}
