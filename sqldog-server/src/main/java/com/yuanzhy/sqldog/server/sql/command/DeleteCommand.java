package com.yuanzhy.sqldog.server.sql.command;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class DeleteCommand extends AbstractSqlCommand {
    public DeleteCommand(String sql) {
        super(sql);
    }

    @Override
    public String execute() {
        // delete from schema.table_name where xxx
        // TODO
        return success();
    }
}