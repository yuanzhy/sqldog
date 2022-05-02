package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.sql.result.SqlResultBuilder;
import com.yuanzhy.sqldog.server.util.Calcites;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.parser.SqlParseException;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/10/31
 */
public class UpdateCommand extends AbstractSqlCommand {
    public UpdateCommand(String sql) {
        super(sql);
    }

    @Override
    public SqlResult execute() {
        // update scheme.table_name set name='zs', age=15 where id = 1
        try {
            SqlNode sqlNode = Calcites.getPanner().parse(sql);
            SqlUpdate update = (SqlUpdate) sqlNode;
//            Calcites.getPanner().validate(update);
            super.parseSchemaTable(update.getTargetTable().toString());
            int rows = table.getTableData().updateBy(update);
            return new SqlResultBuilder(StatementType.DML).schema(currSchema().getName()).table(table.getName()).rows(rows).build();
        } catch (SqlParseException /*| ValidationException*/ e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
        // 1. parser
    }
}
