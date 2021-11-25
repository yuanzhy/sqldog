package com.yuanzhy.sqldog.dialect.pagehelper;

import com.github.pagehelper.dialect.helper.PostgreSqlDialect;
import com.github.pagehelper.page.PageAutoDialect;

/**
 *
 * @author yuanzhy
 * @date 2021-11-24
 */
public class SqldogDialect extends PostgreSqlDialect {

    static {
        try {
            Class<?> cls = Class.forName("com.github.pagehelper.page.PageAutoDialect");
            if (cls != null) {
                PageAutoDialect.registerDialectAlias("sqldog", SqldogDialect.class);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
