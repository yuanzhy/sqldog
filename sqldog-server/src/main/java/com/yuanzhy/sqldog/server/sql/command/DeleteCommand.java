package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.server.util.Calcites;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;

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
        try {
            SqlNode sqlNode = Calcites.getPanner().parse(sql);
//            Calcites.getPanner().validate(sqlNode); // TODO validate
            SqlDelete sqlDelete = (SqlDelete) sqlNode;
            super.parseSchemaTable(sqlDelete.getTargetTable().toString());
            return success(table.getDML().deleteBy(sqlDelete));
        } catch (SqlParseException /*| ValidationException*/ e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }
}
