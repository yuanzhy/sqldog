package com.yuanzhy.sqldog.server.sql.adapter;

import com.yuanzhy.sqldog.core.constant.Consts;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;

import java.io.Reader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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
        if (sqlNode.getKind() == SqlKind.ORDER_BY) {
            SqlOrderBy orderBy = (SqlOrderBy) sqlNode;
            SqlNode query = orderBy.query;
            if (query.getKind() == SqlKind.SELECT) {
                handleSelect((SqlSelect) query);
            }
        } else if (sqlNode.getKind() == SqlKind.UNION) {
            SqlBasicCall basicCall = (SqlBasicCall) sqlNode;
            for (SqlNode oper : basicCall.getOperandList()) {
                if (oper.getKind() == SqlKind.SELECT) {
                    handleSelect((SqlSelect) oper);
                }
            }
        } else if (sqlNode.getKind() == SqlKind.SELECT) {
            handleSelect((SqlSelect) sqlNode);
        } else if (sqlNode.getKind() == SqlKind.INSERT) {
            handleTargetTable(((SqlInsert) sqlNode).getTargetTable());
        } else if (sqlNode.getKind() == SqlKind.UPDATE) {
            handleTargetTable(((SqlUpdate) sqlNode).getTargetTable());
        } else if (sqlNode.getKind() == SqlKind.DELETE) {
            handleTargetTable(((SqlDelete) sqlNode).getTargetTable());
        }
        return sqlNode;
    }

    private void replaceGroupByAlias(SqlSelect sqlSelect) {
        SqlNodeList groupBy = sqlSelect.getGroup();
        SqlNodeList selectList = sqlSelect.getSelectList();
        if (groupBy != null && selectList != null && selectList.size() > 0) {
            Map<String, Integer> nameOrdinal = new HashMap<>();
            for (int i = 0; i < selectList.size(); i++) {
                SqlNode select = selectList.get(i);
                if (select instanceof SqlBasicCall
                        && ((SqlBasicCall) select).getOperator().getKind() == SqlKind.AS
                        && ((SqlBasicCall) select).getOperandList().size() == 2) {
                    SqlNode selectAs = ((SqlBasicCall) select).getOperandList().get(1);
                    if (selectAs instanceof SqlIdentifier) {
                        nameOrdinal.put(((SqlIdentifier) selectAs).names.get(0), i);
                    }
                }
            }
            for (int i = 0; i < groupBy.size(); i++) {
                if (groupBy.get(i) instanceof SqlIdentifier) {
                    SqlIdentifier groupByIdty = (SqlIdentifier) groupBy.get(i);
                    if (groupByIdty.names.size() == 1) {
                        Integer ordinal = nameOrdinal.get(groupByIdty.names.get(0));
                        if (ordinal != null) {
                            groupBy.set(i, SqlLiteral.createExactNumeric(ordinal.toString(), SqlParserPos.ZERO));
                        }
                    }
                }
            }
        }
    }

    /**
     * 自动拼接schema
     * @param targetTable targetTable
     */
    private void handleTargetTable(SqlNode targetTable) {
        if (targetTable != null && schema != null) {
            if (targetTable instanceof SqlIdentifier) {
                handleIdentifier((SqlIdentifier)targetTable);
            } else if (targetTable instanceof SqlBasicCall) {
                handleBasicCall((SqlBasicCall) targetTable);
            } else if (targetTable instanceof SqlJoin) {
                handleJoin((SqlJoin) targetTable);
            }
        }
    }

    /**
     * 自动拼接上 schema
     * @param sqlSelect sqlSelect
     */
    private void handleSelect(SqlSelect sqlSelect) {
        handleTargetTable(sqlSelect.getFrom());
        if (sqlSelect.getWhere() instanceof SqlBasicCall) {
            handleBasicCall((SqlBasicCall) sqlSelect.getWhere());
            // 处理where条件中的日期
//            handleCondition((SqlBasicCall) sqlSelect.getWhere());
        }
        replaceGroupByAlias(sqlSelect);
    }

//    private void handleCondition(SqlBasicCall condition) {
//        SqlKind kind = condition.getOperator().getKind();
//        SqlNode left = condition.getOperandList().get(0);
//        SqlNode right = condition.getOperandList().size() > 1 ? condition.getOperandList().get(1): null;
//        if (kind == SqlKind.AND) {
//            if (left instanceof SqlBasicCall) {
//                handleCondition((SqlBasicCall) left);
//            }
//            if (right instanceof SqlBasicCall) {
//                handleCondition((SqlBasicCall) right);
//            }
//        } else if (kind == SqlKind.OR) {
//            if (left instanceof SqlBasicCall) {
//                handleCondition((SqlBasicCall) left);
//            }
//            if (right instanceof SqlBasicCall) {
//                handleCondition((SqlBasicCall) right);
//            }
//        } else {
//            if (right instanceof SqlTimestampLiteral) {
//                SqlTimestampLiteral tsLiteral =  (SqlTimestampLiteral) right;
//                String tsStr = tsLiteral.toValue();
//                Date d = DateUtils.addHours(DateUtil.parseDatetime(tsStr), -8);
//                TimestampString ts = TimestampString.fromMillisSinceEpoch(d.getTime());
//                condition.setOperand(1, SqlLiteral.createTimestamp(ts, 0, SqlParserPos.ZERO));
//            } else if (right instanceof SqlTimeLiteral) {
//                SqlTimeLiteral tsLiteral =  (SqlTimeLiteral) right;
//                String tsStr = tsLiteral.toValue();
//                Date d = DateUtils.addHours(DateUtil.parseSqlTime(tsStr), -8);
//                TimeString ts = TimeString.fromMillisOfDay((int)d.getTime());
//                condition.setOperand(1, SqlLiteral.createTime(ts, 0, SqlParserPos.ZERO));
//            } else if (right instanceof SqlDateLiteral) {
//                SqlDateLiteral tsLiteral = (SqlDateLiteral) right;
//                String tsStr = tsLiteral.toValue();
//                Date d = DateUtils.addHours(DateUtil.parseSqlDate(tsStr), -8);
//                Calendar cal = Calendar.getInstance();
//                cal.setTimeInMillis(d.getTime());
//                DateString ts = DateString.fromCalendarFields(cal);
//                condition.setOperand(1, SqlLiteral.createDate(ts, SqlParserPos.ZERO));
//            }
//        }
//
//    }

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
        } else {
            for (SqlNode sqlNode : sqlBasicCall.getOperandList()) {
                if (sqlNode.getKind() == SqlKind.SELECT) {
                    handleSelect((SqlSelect) sqlNode);
                }
            }
        }
    }

    private void handleIdentifier(SqlIdentifier sqlIdentifier) {
        if (sqlIdentifier.names.size() == 1 && !sqlIdentifier.names.get(0).startsWith(Consts.SYSTABLE_PREFIX)) {
            // 如果不带模式， 则自动拼接上
            sqlIdentifier.setNames(Arrays.asList(new String[]{ schema, sqlIdentifier.names.get(0)}), null);
        }
    }
}
