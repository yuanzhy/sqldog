package com.yuanzhy.sqldog.server.sql.adapter;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2021/12/4
 */
@Deprecated // 和ScannableCalciteTable配套使用的，直接遍历内存全量数据，没有任何优化
public class ObjectArrayEnumerable extends AbstractEnumerable<Object[]> {

    private final AtomicBoolean cancelFlag;
    private final List<Object[]> data;

    public ObjectArrayEnumerable(DataContext root, List<Object[]> data) {
        cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
        this.data = data;
    }

    @Override
    public Enumerator<Object[]> enumerator() {
        return new Enumerator<Object[]>() {
            private int index = -1;

            @Override
            public Object[] current() {
                Object[] current = data.get(this.index);
                return current;
                //return current != null && current.getClass().isArray() ? (Object[])(current) : new Object[]{current};
            }

            @Override
            public boolean moveNext() {
                if (cancelFlag != null && cancelFlag.get()) {
                    return false;
                } else {
                    return ++this.index < data.size();
                }
            }

            @Override
            public void reset() {
                this.index = -1;
            }

            @Override
            public void close() {

            }
        };
    }
}
