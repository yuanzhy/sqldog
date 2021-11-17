package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.sql.SqlResultBuilder;
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
    public SqlResult execute() {
        try {
            SqlNode sqlNode = Calcites.getPanner().parse(sql);
//            Calcites.getPanner().validate(sqlNode); // TODO validate
            SqlDelete sqlDelete = (SqlDelete) sqlNode;
            super.parseSchemaTable(sqlDelete.getTargetTable().toString());
            int rows = table.getDML().deleteBy(sqlDelete);
            return new SqlResultBuilder(StatementType.DML).schema(schema.getName()).table(table.getName()).rows(rows).build();
        } catch (SqlParseException /*| ValidationException*/ e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }
}
