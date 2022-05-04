package com.yuanzhy.sqldog.server.sql.adapter;

import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/5/3
 */
public class DogTableScan extends AbstractRelNode /* TableScan */ implements EnumerableRel {
    /**
     * Creates an <code>AbstractRelNode</code>.
     *
     * @param cluster
     * @param traitSet
     */
    protected DogTableScan(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet);
    }

//    protected DogTableScan(RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelOptTable table) {
//        super(cluster, traitSet, hints, table);
//    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        return null;
    }
}
