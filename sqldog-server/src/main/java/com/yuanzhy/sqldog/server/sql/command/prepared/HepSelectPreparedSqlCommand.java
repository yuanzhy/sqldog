package com.yuanzhy.sqldog.server.sql.command.prepared;

import java.sql.PreparedStatement;

import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelRunner;

import com.yuanzhy.sqldog.server.sql.PreparedSqlCommand;
import com.yuanzhy.sqldog.server.util.Calcites;

/**
 * 基于 HepPlanner 优化器的dql
 * @author yuanzhy
 * @since 0.2
 * @date 2022/9/17
 */
public class HepSelectPreparedSqlCommand extends SelectPreparedSqlCommand implements PreparedSqlCommand {

    public HepSelectPreparedSqlCommand(String preparedSql) {
        super(preparedSql);
    }

    @Override
    protected PreparedStatement precompile(String preparedSql) {
        try {
            Planner planner = Calcites.getPanner();
            SqlNode sqlNode = planner.parse(preparedSql);
            // 2. validate
            planner.validate(sqlNode);
            // 3. Logical Plan
            RelRoot relRoot = planner.rel(sqlNode);
            RelNode relNode = relRoot.project();
//            System.out.println(relNode.explain());
            // 4. 优化
            HepProgramBuilder builder = new HepProgramBuilder();
            builder.addRuleInstance(CoreRules.PROJECT_FILTER_TRANSPOSE)
                    .addRuleInstance(CoreRules.FILTER_MERGE)
                    .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
                    .addRuleInstance(CoreRules.FILTER_AGGREGATE_TRANSPOSE)
                    .addRuleInstance(CoreRules.PROJECT_MERGE)
                    .addRuleInstance(CoreRules.PROJECT_REMOVE)
                    .addRuleInstance(CoreRules.PROJECT_JOIN_TRANSPOSE)
                    .addRuleInstance(CoreRules.PROJECT_SET_OP_TRANSPOSE)
                    .addRuleInstance(CoreRules.FILTER_TO_CALC)
                    .addRuleInstance(CoreRules.FILTER_CALC_MERGE)
                    .addRuleInstance(CoreRules.PROJECT_CALC_MERGE)
                    .addRuleInstance(CoreRules.CALC_MERGE)
                    .build();
            HepPlanner hepPlanner = new HepPlanner(builder.build());
            hepPlanner.setRoot(relNode);
            relNode = hepPlanner.findBestExp();
            final RelRunner runner = Calcites.getConnection().unwrap(RelRunner.class);
            return runner.prepareStatement(relNode);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
