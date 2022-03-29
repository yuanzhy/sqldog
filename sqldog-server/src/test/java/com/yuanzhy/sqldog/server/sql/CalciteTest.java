package com.yuanzhy.sqldog.server.sql;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.junit.Test;

import com.yuanzhy.sqldog.server.core.Table;
import com.yuanzhy.sqldog.server.core.constant.ConstraintType;
import com.yuanzhy.sqldog.server.core.constant.DataType;
import com.yuanzhy.sqldog.server.storage.builder.ColumnBuilder;
import com.yuanzhy.sqldog.server.storage.builder.ConstraintBuilder;
import com.yuanzhy.sqldog.server.storage.builder.TableBuilder;
import com.yuanzhy.sqldog.server.sql.adapter.CalciteTable;

/**
 *
 * @author yuanzhy
 * @date 2021-10-25
 */
public class CalciteTest {

    @Test
    public void t() throws SqlParseException, RelConversionException {
        Table table = new TableBuilder().name("TEST")
                .addColumn(new ColumnBuilder().name("ID").dataType(DataType.INT).nullable(false).build())
                .addColumn(new ColumnBuilder().name("NAME").dataType(DataType.VARCHAR).precision(50).build())
                .addColumn(new ColumnBuilder().name("AGE").dataType(DataType.INT).build())
                .addColumn(new ColumnBuilder().name("BIRTH").dataType(DataType.DATE).build())
                .addConstraint(new ConstraintBuilder().type(ConstraintType.PRIMARY_KEY).addColumnName("ID").build())
                .build();

        SchemaPlus schemaPlus = Frameworks.createRootSchema(true);
        schemaPlus.add("TEST", new CalciteTable(table));
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(schemaPlus)
                .parserConfig(SqlParser.config()
                        .withParserFactory(SqlParserImpl.FACTORY)
                        .withCaseSensitive(false)
                        //.withQuoting(Quoting.BACK_TICK)
                        //.withQuotedCasing(Casing.TO_UPPER)
                        //.withUnquotedCasing(Casing.TO_UPPER)
                        //.withConformance(SqlConformanceEnum.ORACLE_12)
                ).build();
        String sql = "select max(id), max(name) from test where id < 5 and name = 'zhang' or birth>'2020-10-10' group by age having age>5 order by age desc limit 3 offset 1";
        //String sql = "select ids, name from test where id < 5 and name = 'zhang' or c=1 group by a,b having c=4 limit 3 offset 1";
//        sql = "create table test {id int not null, name varchar(50) default '1'}";
//        sql = "alter table test add column name2 varchar(50) default '1'";
        //SqlParser parser = SqlParser.create(sql, config.getParserConfig());
        //SqlNode sqlNode = parser.parseQuery();
        Planner planner = Frameworks.getPlanner(config);
        // 1. parser
        SqlNode sqlNode = planner.parse(sql);
        if (sqlNode.getKind() == SqlKind.SELECT) {
            SqlSelect select = (SqlSelect) sqlNode;
            SqlNodeList selectList = select.getSelectList(); // selectList.get(0).toString();
            SqlIdentifier from = (SqlIdentifier) select.getFrom();
            SqlBasicCall where = (SqlBasicCall) select.getWhere();
        } else if (sqlNode.getKind() == SqlKind.ORDER_BY) {
            SqlOrderBy orderBy = (SqlOrderBy) sqlNode;
            Integer limit = ((SqlNumericLiteral)orderBy.fetch).getPrec();
            Integer offset = ((SqlNumericLiteral)orderBy.offset).getPrec();
            //SqlBasicCall order0 = (SqlBasicCall)orderBy.orderList.get(0);
            SqlSelect select = (SqlSelect) orderBy.query;
            SqlNodeList selectList = select.getSelectList(); // selectList.get(0).toString();
            SqlIdentifier from = (SqlIdentifier) select.getFrom();
            SqlBasicCall where = (SqlBasicCall) select.getWhere();
            //SqlIdentifier group0 = (SqlIdentifier) select.getGroup().get(0);
            //SqlIdentifier group1 = (SqlIdentifier) select.getGroup().get(1);
            SqlBasicCall having = (SqlBasicCall) select.getHaving();
            having.getOperator(); // "="
            having.getOperandList(); // ["C", "4"]
        }
        // 2. validate
        try {
            planner.validate(sqlNode);
        } catch (ValidationException e) {
            e.printStackTrace();
        }
        System.out.println(sqlNode);
        System.out.println("=====================");
        // 3. Logical Plan
        RelRoot relRoot = planner.rel(sqlNode);
        RelNode relNode = relRoot.project();
        planner.close();
        System.out.println(relNode.explain());
        System.out.println("=====================");
        // 4. 优化
        //HepProgramBuilder builder = new HepProgramBuilder();
        //builder.addRuleInstance(CoreRules.FILTER_INTO_JOIN); //note: 添加 rule
        //HepPlanner hepPlanner = new HepPlanner(builder.build());
        //hepPlanner.setRoot(relNode);
        //relNode = hepPlanner.findBestExp();
        //System.out.println(RelOptUtil.toString(relNode));
    }
}
