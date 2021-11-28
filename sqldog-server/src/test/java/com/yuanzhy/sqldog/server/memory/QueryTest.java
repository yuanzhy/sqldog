package com.yuanzhy.sqldog.server.memory;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/11/14
 */
public class QueryTest {

    @Test
    public List<Map<String, Object>> selectBy(SqlNode sqlNode) {
        handleSQL(sqlNode);
        return null;
    }

    private void handleSQL(SqlNode sqlNode) {
        SqlKind kind = sqlNode.getKind();
        switch (kind) {
            case SELECT:
                handleSelect(sqlNode);
                break;
            case UNION:
                ((SqlBasicCall) sqlNode).getOperandList().forEach(node -> {
                    handleSQL(node);
                });
                break;
            case ORDER_BY:
                handleOrderBy(sqlNode);
                break;
        }
    }

    private void handleOrderBy(SqlNode node) {
        SqlOrderBy sqlOrderBy = (SqlOrderBy) node;
        SqlNode query = sqlOrderBy.query;
        handleSQL(query);
        SqlNodeList orderList = sqlOrderBy.orderList;
        handlerField(orderList);

        SqlNode fetch = sqlOrderBy.fetch;
        SqlNode offset = sqlOrderBy.offset;
    }


    private void handleSelect(SqlNode select) {
        SqlSelect sqlSelect = (SqlSelect) select;
        SqlNodeList selectList = sqlSelect.getSelectList();
        //字段信息
        selectList.getList().forEach(list -> {
            handlerField(list);
        });

        handlerFrom(sqlSelect.getFrom());

        if (sqlSelect.hasWhere()) {
            handlerField(sqlSelect.getWhere());
        }

        if (sqlSelect.hasOrderBy()) {
            handlerField(sqlSelect.getOrderList());
        }

        SqlNodeList group = sqlSelect.getGroup();
        if (group != null) {
            group.forEach(groupField -> {
                handlerField(groupField);
            });
        }


        SqlNode fetch = sqlSelect.getFetch();
        if (fetch != null) {
        }

    }

    private void handlerFrom(SqlNode from) {
        SqlKind kind = from.getKind();

        switch (kind) {
            case IDENTIFIER:
                //最终的表名
                SqlIdentifier sqlIdentifier = (SqlIdentifier) from;
                System.out.println("==tablename===" + sqlIdentifier.toString());
                break;
            case AS:
                SqlBasicCall sqlBasicCall = (SqlBasicCall) from;
                SqlNode selectNode = sqlBasicCall.getOperandList().get(0);
                handleSQL(selectNode);
                break;
            case JOIN:
                SqlJoin sqlJoin = (SqlJoin) from;
                SqlNode left = sqlJoin.getLeft();
                handleSQL(left);
                SqlNode right = sqlJoin.getRight();
                handleSQL(right);
                SqlNode condition = sqlJoin.getCondition();
                handlerField(condition);
                break;
            case SELECT:
                handleSQL(from);
                break;
        }
    }

    private void handlerField(SqlNode field) {
        SqlKind kind = field.getKind();
        switch (kind) {
            case AS:
                SqlNode[] operands_as = ((SqlBasicCall) field).operands;
                SqlNode left_as = operands_as[0];
                handlerField(left_as);
                break;
            case IDENTIFIER:
                //表示当前为子节点
                SqlIdentifier sqlIdentifier = (SqlIdentifier) field;
                System.out.println("===field===" + sqlIdentifier.toString());
                break;
            default:
                if (field instanceof SqlBasicCall) {
                    SqlNode[] nodes = ((SqlBasicCall) field).operands;
                    for (int i = 0; i < nodes.length; i++) {
                        handlerField(nodes[i]);
                    }
                }
                if (field instanceof SqlNodeList) {
                    ((SqlNodeList) field).getList().forEach(node -> {
                        handlerField(node);
                    });
                }
                break;
        }
    }
}
