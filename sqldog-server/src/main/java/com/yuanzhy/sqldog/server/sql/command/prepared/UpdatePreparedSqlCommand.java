package com.yuanzhy.sqldog.server.sql.command.prepared;

import com.yuanzhy.sqldog.core.sql.ColumnMetaData;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.sql.PreparedSqlCommand;
import com.yuanzhy.sqldog.server.sql.command.AbstractSqlCommand;
import com.yuanzhy.sqldog.server.util.Calcites;
import com.yuanzhy.sqldog.server.util.Databases;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlUpdate;

import java.io.IOException;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/22
 */
public class UpdatePreparedSqlCommand extends AbstractSqlCommand implements PreparedSqlCommand {
    private final String preparedSql;
    public UpdatePreparedSqlCommand(String preparedSql) {
        super(preparedSql);
        this.preparedSql = preparedSql;
    }

    @Override
    public SqlResult execute(Object[] parameter) {
        // TODO
        return null;
    }

    @Override
    public SqlResult execute() {
        // TODO
        try {
            SqlUpdate sqlUpdate = (SqlUpdate) Calcites.getPanner().parse(preparedSql);
            super.parseSchemaTable(sqlUpdate.getTargetTable().toString());
            SqlNodeList targetColumnList = sqlUpdate.getTargetColumnList();
            ColumnMetaData[] columns = new ColumnMetaData[targetColumnList.size()];
//            targetColumnList
            System.out.println(sqlUpdate);
        } catch (Exception e) {

        }
        return null;
    }

    @Override
    public void currentSchema(String schema) {
        Databases.currSchema(schema);
    }

    @Override
    public void close() throws IOException {
        // noop
    }
}
