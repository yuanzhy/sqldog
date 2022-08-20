package com.yuanzhy.sqldog.dialect.spring.jdbc;

import org.springframework.data.relational.core.dialect.AbstractDialect;
import org.springframework.data.relational.core.dialect.LimitClause;
import org.springframework.data.relational.core.dialect.LockClause;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/8/20
 */
public class SqldogDialect extends AbstractDialect {

    public static final SqldogDialect INSTANCE = new SqldogDialect();

    protected SqldogDialect() {}

    private static final LimitClause LIMIT_CLAUSE = new LimitClause() {

        /*
         * (non-Javadoc)
         * @see org.springframework.data.relational.core.dialect.LimitClause#getLimit(long)
         */
        @Override
        public String getLimit(long limit) {
            return "LIMIT " + limit;
        }

        /*
         * (non-Javadoc)
         * @see org.springframework.data.relational.core.dialect.LimitClause#getOffset(long)
         */
        @Override
        public String getOffset(long offset) {
            return "OFFSET " + offset;
        }

        /*
         * (non-Javadoc)
         * @see org.springframework.data.relational.core.dialect.LimitClause#getClause(long, long)
         */
        @Override
        public String getLimitOffset(long limit, long offset) {
            return String.format("LIMIT %d OFFSET %d", limit, offset);
        }

        /*
         * (non-Javadoc)
         * @see org.springframework.data.relational.core.dialect.LimitClause#getClausePosition()
         */
        @Override
        public Position getClausePosition() {
            return Position.AFTER_ORDER_BY;
        }
    };

    /*
     * (non-Javadoc)
     * @see org.springframework.data.relational.core.dialect.Dialect#limit()
     */
    @Override
    public LimitClause limit() {
        return LIMIT_CLAUSE;
    }


    /*
     * (non-Javadoc)
     * @see org.springframework.data.relational.core.dialect.Dialect#lock()
     */
    @Override
    public LockClause lock() {
        return null;
    }
}
