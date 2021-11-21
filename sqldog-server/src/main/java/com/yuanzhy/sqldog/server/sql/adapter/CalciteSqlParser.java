package com.yuanzhy.sqldog.server.sql.adapter;

import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;

import java.io.Reader;
import java.util.Arrays;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/15
 */
public class CalciteSqlParser extends SqlParserImpl {
    private final String schema;
    public CalciteSqlParser(Reader reader, String schema) {
        super(reader);
        this.schema = schema;
    }

    @Override
    public SqlNode parseSqlStmtEof() throws Exception {
        SqlNode sqlNode = super.parseSqlStmtEof();
        SqlNode targetTable = null;
        if (sqlNode.getKind() == SqlKind.ORDER_BY) {
            SqlOrderBy orderBy = (SqlOrderBy) sqlNode;
            SqlNode query = orderBy.query;
            if (query.getKind() == SqlKind.SELECT) {
                SqlSelect sqlSelect = (SqlSelect) query;
                targetTable = sqlSelect.getFrom();
            }
        }

        if (sqlNode.getKind() == SqlKind.SELECT) {
            SqlSelect sqlSelect = (SqlSelect) sqlNode;
            targetTable = sqlSelect.getFrom();
        } else if (sqlNode.getKind() == SqlKind.INSERT) {
            targetTable = ((SqlInsert) sqlNode).getTargetTable();
        } else if (sqlNode.getKind() == SqlKind.UPDATE) {
            targetTable = ((SqlUpdate) sqlNode).getTargetTable();
        } else if (sqlNode.getKind() == SqlKind.DELETE) {
            targetTable = ((SqlDelete) sqlNode).getTargetTable();
        }
        if (targetTable != null) {
            if (targetTable instanceof SqlIdentifier) {
                handleIdentifier((SqlIdentifier)targetTable);
            } else if (targetTable instanceof SqlBasicCall) {
                handleBasicCall((SqlBasicCall) targetTable);
            } else if (targetTable instanceof SqlJoin) {
                handleJoin((SqlJoin) targetTable);
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
            sqlIdentifier.setNames(Arrays.asList(new String[]{ schema, sqlIdentifier.names.get(0)}), null);
        }
    }
}
