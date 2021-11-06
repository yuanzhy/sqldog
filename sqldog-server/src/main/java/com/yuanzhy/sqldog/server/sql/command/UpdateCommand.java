package com.yuanzhy.sqldog.server.sql.command;

import com.yuanzhy.sqldog.server.util.Calcites;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.parser.SqlParseException;

import java.util.HashMap;
import java.util.Map;

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
    public String execute() {
        // update scheme.table_name set name='ls', age=15 where id = 1
        try {
            SqlNode sqlNode = Calcites.getPanner().parse(sql);
//            Calcites.getPanner().validate(sqlNode);
            SqlUpdate update = (SqlUpdate) sqlNode;
//            super.parseSchemaTable(update.getTargetTable().toString());
            SqlNodeList colList = update.getTargetColumnList();
            SqlNodeList valList = update.getSourceExpressionList();
            Map<String, Object> updates = new HashMap<>();
            for (int i = 0; i < colList.size(); i++) {
                String colName = colList.get(i).toString().toUpperCase();
                SqlNode valNode = valList.get(i);
                if (valNode.getKind() != SqlKind.LITERAL) {
                    throw new UnsupportedOperationException(valNode + " not supported");
                }
                Object value = ((SqlLiteral) valNode).getValue();
                updates.put(colName, value);
            }
            Map<String, Object> wheres = new HashMap<>();
            // TODO
            System.out.println(sqlNode);
            return success();
        } catch (SqlParseException /*| ValidationException*/ e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
        // 1. parser
    }

    public static void main(String[] args) {
        UpdateCommand u = new UpdateCommand("update scheme.table_name set name='ls', age=15 where id = 0 and bb='2020-10-11' and (c=1 or d=2)");
        u.execute();
    }
}
