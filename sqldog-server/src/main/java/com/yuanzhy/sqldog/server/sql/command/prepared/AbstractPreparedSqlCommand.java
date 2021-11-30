package com.yuanzhy.sqldog.server.sql.command.prepared;

import java.io.IOException;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.yuanzhy.sqldog.core.constant.StatementType;
import com.yuanzhy.sqldog.core.sql.SqlResult;
import com.yuanzhy.sqldog.server.sql.PreparedSqlCommand;
import com.yuanzhy.sqldog.server.sql.result.SqlResultBuilder;
import com.yuanzhy.sqldog.server.util.Calcites;
import com.yuanzhy.sqldog.server.util.Databases;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/21
 */
public abstract class AbstractPreparedSqlCommand implements PreparedSqlCommand {

    protected final String preparedSql;
//    protected String currentSchema;
    protected PreparedStatement ps;
    public AbstractPreparedSqlCommand(String preparedSql) {
        this.preparedSql = preparedSql;
        try {
            ps = Calcites.getConnection().prepareStatement(preparedSql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void currentSchema(String schema) {
        Databases.currSchema(schema);
        //if (schema != null) {
        //    this.schema = Databases.getDefault().getSchema(schema);
        //    checkSchema();
        //}
    }

    @Override
    public SqlResult execute() {
        try {
            ResultSetMetaData rsmd = ps.getMetaData();
            ParameterMetaData pmd = ps.getParameterMetaData();
            StatementType t = this instanceof SelectPreparedSqlCommand ? StatementType.DQL : StatementType.DML;
            return new SqlResultBuilder(t).params(pmd).columns(rsmd).build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            ps = null;
        }
    }
}
