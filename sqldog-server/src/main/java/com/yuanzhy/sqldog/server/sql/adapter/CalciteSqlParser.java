package com.yuanzhy.sqldog.server.sql.adapter;

import com.yuanzhy.sqldog.server.util.Databases;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;

import java.io.Reader;
import java.util.Arrays;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/15
 */
public class CalciteSqlParser extends SqlParserImpl {
    public CalciteSqlParser(Reader reader) {
        super(reader);
    }

    @Override
    public SqlNode parseSqlStmtEof() throws Exception {
        SqlNode sqlNode = super.parseSqlStmtEof();
        SqlSelect sqlSelect = null;
        if (sqlNode.getKind() == SqlKind.ORDER_BY) {
            SqlOrderBy orderBy = (SqlOrderBy) sqlNode;
            SqlNode query = orderBy.query;
            if (query.getKind() == SqlKind.SELECT) {
                sqlSelect = (SqlSelect) query;
            }
        }
        if (sqlNode.getKind() == SqlKind.SELECT) {
            sqlSelect = (SqlSelect) sqlNode;
        }
        if (sqlSelect != null) {
            SqlNode sqlFrom = sqlSelect.getFrom();
            if (sqlFrom instanceof SqlIdentifier) {
                handleIdentifier((SqlIdentifier)sqlFrom);
            } else if (sqlFrom instanceof SqlBasicCall) {
                handleBasicCall((SqlBasicCall) sqlFrom);
            } else if (sqlFrom instanceof SqlJoin) {
                handleJoin((SqlJoin) sqlFrom);
            }
        }
        return sqlNode;
    }

    private void handleJoin(SqlJoin sqlJoin) {
        if (sqlJoin.getLeft() instanceof SqlBasicCall) {
            handleBasicCall((SqlBasicCall)sqlJoin.getLeft());
        } else if (sqlJoin.getLeft() instanceof SqlJoin) {
            handleJoin((SqlJoin)sqlJoin.getLeft());
        }
        if (sqlJoin.getRight() instanceof SqlBasicCall) {
            handleBasicCall((SqlBasicCall)sqlJoin.getRight());
        } else if (sqlJoin.getRight() instanceof SqlJoin) {
            handleJoin((SqlJoin)sqlJoin.getRight());
        }
    }

    private void handleBasicCall(SqlBasicCall sqlBasicCall) {
        if (sqlBasicCall.getOperator() instanceof SqlAsOperator) {
            SqlNode o1 = sqlBasicCall.getOperandList().get(0);
            if (o1 instanceof SqlIdentifier) {
                handleIdentifier((SqlIdentifier)o1);
            }
        }
    }

    private void handleIdentifier(SqlIdentifier sqlIdentifier) {
        if (sqlIdentifier.names.size() == 1) {
            // 如果不带模式， 则自动拼接上
            sqlIdentifier.setNames(Arrays.asList(new String[]{Databases.currSchema().getName(), sqlIdentifier.names.get(0)}), null);
        }
    }
}
