package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.server.util.Databases;

import java.util.stream.Collectors;

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
    public String execute() { // TODO 表格形式展现
        String sqlSuffix = sql.substring("SHOW ".length());
        if ("DATABASES".equals(sqlSuffix)) {
            return Databases.getDefault().getName();
        } else if ("SCHEMAS".equals(sqlSuffix)) {
            return Databases.getDefault().getSchemaNames().stream().collect(Collectors.joining("\n"));
        } else if ("TABLES".equals(sqlSuffix)) {
//            Databases.getDefault().getSchema();
            // TODO
            return null;
        } else {
            throw new UnsupportedOperationException("not supported: " + sql);
        }
    }
}
