package com.yuanzhy.sqldog.dialect.pagehelper;

import com.github.pagehelper.dialect.helper.PostgreSqlDialect;
import com.github.pagehelper.page.PageAutoDialect;

/**
 *
 * @author yuanzhy
 * @date 2021-11-24
 */
public class SqldogDialect extends PostgreSqlDialect {

    public SqldogDialect() {
        PageAutoDialect.registerDialectAlias("sqldog", SqldogDialect.class);
    }
}
