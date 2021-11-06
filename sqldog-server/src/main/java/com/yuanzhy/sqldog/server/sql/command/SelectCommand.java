package com.yuanzhy.sqldog.server.sql.command;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class SelectCommand extends AbstractSqlCommand {
    public SelectCommand(String sql) {
        super(sql);
    }

    @Override
    public String execute() {
        // TODO
        return success();
    }
}
