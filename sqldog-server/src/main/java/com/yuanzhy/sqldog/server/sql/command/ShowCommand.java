package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.server.util.Databases;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/6
 */
public class ShowCommand extends AbstractSqlCommand {
    public ShowCommand(String sql) {
        super(sql);
    }

    @Override
    public String execute() {
        String sqlSuffix = sql.substring("SHOW ".length());
        if ("DATABASES".equals(sqlSuffix)) {
            return Databases.getDefault().getName();
        } else if ("SCHEMAS".equals(sqlSuffix)) {
            return Databases.getDefault().toPrettyString();
        } else if ("TABLES".equals(sqlSuffix)) {
            return Databases.currSchema().toPrettyString();
        } else if ("SEARCH_PATH".equals(sqlSuffix)) {
            return Databases.currSchema().getName();
        } else {
            throw new UnsupportedOperationException("not supported: " + sql);
        }
    }
}
