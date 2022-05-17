package com.yuanzhy.sqldog.server.sql.adapter;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * @author yuanzhy
 * @version 1.0
 * @date 2022/5/3
 */
class FilterableEnumerator implements Enumerator<Object[]> {

    private static final Logger LOG = LoggerFactory.getLogger(FilterableEnumerator.class);
    private static final Predicate<Object[]> TRUE_PREDICATE = t -> true;
//    private final Table table;
    private final Iterable<Object[]> iterable;
    private final AtomicBoolean cancelFlag;
    private Iterator<Object[]> iterator;
    private Object[] current;
    private Predicate<Object[]> predicate = TRUE_PREDICATE;

    FilterableEnumerator(DataContext root, Iterable<Object[]> iterable, List<RexNode> filters) {
//        this.table = table;
//        columnArray = table.getColumns().values().toArray(new Column[0]);
        this.iterable = iterable;
        iterator = iterable.iterator();
        current = null;
        this.cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);

        if (filters.size() > 1) {
            throw new RuntimeException("filters.size() > 1");
        }
        for (RexNode filter : filters) {
            if (filter instanceof RexCall) {
                predicate = handleWhere((RexCall) filter);
            } else {
                LOG.warn("not supported filters: " + filter);
            }
        }
    }

    @Override public Object[] current() {
        if (current == null) {
            throw new NoSuchElementException();
        }
        return current;
    }

    @Override public boolean moveNext() {
        if (cancelFlag != null && cancelFlag.get()) {
            current = null;
            close();
            return false;
        }
        if (!iterator.hasNext()) {
            current = null;
            return false;
        }
        current = iterator.next();
        if (predicate.test(current)) {
            return true;
        }
        return moveNext();
    }

    @Override public void reset() {
        iterator = this.iterable.iterator();
        current = null;
    }

    @Override public void close() {
        final Iterator<Object[]> iterator1 = this.iterator;
        this.iterator = null;
        closeIterator(iterator1);
    }

    private Predicate<Object[]> handleWhere(RexCall condition) {
        SqlKind kind = condition.getOperator().getKind();
        RexNode left = condition.getOperands().get(0);
        RexNode right = condition.getOperands().size() > 1 ? condition.getOperands().get(1): null;
        if (kind == SqlKind.AND) {
            Predicate<Object[]> leftPredicate, rightPredicate;
            if (left instanceof RexCall) {
                leftPredicate = handleWhere((RexCall) left);
            } else {
                LOG.warn("operation not support: " + left.toString());
                return TRUE_PREDICATE;
            }
            if (right instanceof RexCall) {
                rightPredicate = handleWhere((RexCall) right);
            } else {
                LOG.warn("operation not support: " + right.toString());
                return TRUE_PREDICATE;
            }
            return leftPredicate.and(rightPredicate);
        } else if (kind == SqlKind.OR) {
            Predicate<Object[]> leftPredicate, rightPredicate;
            if (left instanceof RexCall) {
                leftPredicate = handleWhere((RexCall) left);
            } else {
                LOG.warn("operation not support: " + left.toString());
                return TRUE_PREDICATE;
            }
            if (right instanceof RexCall) {
                rightPredicate = handleWhere((RexCall) right);
            } else {
                LOG.warn("operation not support: " + right.toString());
                rightPredicate = TRUE_PREDICATE;
            }
            return leftPredicate.or(rightPredicate);
        }
        if (!(left instanceof RexInputRef)) {
            // 暂不支持复杂的where条件，例
            // ID + AGE > 15
            // ID + 2 > 15  等等
//            throw new UnsupportedOperationException("operation not support: " + left.toString());
            LOG.warn("operation not support: " + left.toString());
            return TRUE_PREDICATE;
        } else if (right == null || right instanceof RexDynamicParam) {
            return TRUE_PREDICATE;
        }
        final int colIndex = ((RexInputRef)left).getIndex();
        Predicate<Object[]> fn = TRUE_PREDICATE;
        Comparable rexVal = parseValue(right);
        if (kind == SqlKind.EQUALS) {
            fn = m -> compare(m[colIndex], rexVal) == 0;
        } else if (kind == SqlKind.NOT_EQUALS) {
            fn = m -> rexVal != null && compare(m[colIndex], rexVal) != 0;
        }
//        else if (kind == SqlKind.IN) {
//            fn = m -> m[colIndex] != null && ((List)rexVal).contains(m[colIndex]);
//        }
        else if (kind == SqlKind.NOT_IN) {
            fn = m -> m[colIndex] != null && !((List)rexVal).contains(m[colIndex]);
        } /*else if (kind == SqlKind.EXISTS) {

        } */else if (kind == SqlKind.IS_NULL) {
            fn = m -> m[colIndex] == null;
        } else if (kind == SqlKind.IS_NOT_NULL) {
            fn = m -> m[colIndex] != null;
        } else if (kind == SqlKind.LESS_THAN) {
            fn = m -> m[colIndex] != null && compare(m[colIndex], rexVal) < 0;
        } else if (kind == SqlKind.LESS_THAN_OR_EQUAL) {
            fn = m -> m[colIndex] != null && compare(m[colIndex], rexVal) <= 0;
        } else if (kind == SqlKind.GREATER_THAN) {
            fn = m -> m[colIndex] != null && compare(m[colIndex], rexVal) > 0;
        } else if (kind == SqlKind.GREATER_THAN_OR_EQUAL) {
            fn = m -> m[colIndex] != null && compare(m[colIndex], rexVal) >= 0;
        } else if (kind == SqlKind.LIKE) {
            if (rexVal == null) {
                fn = m -> false;
            } else {
                String rexStr;
                if (rexVal instanceof NlsString) {
                    rexStr = ((NlsString) rexVal).getValue();
                } else {
                    rexStr = rexVal.toString();
                }
                Pattern pattern = Pattern.compile("^" + rexStr.replace("%", ".*").replace("_", ".") + "$");
                fn = m -> {
                    Object v = m[colIndex];
                    if (v == null) {
                        return false;
                    }
                    return pattern.matcher(v.toString()).matches();
                };
            }
        } else if (kind == SqlKind.SEARCH) {
            RangeSet<Comparable> rangeSet = ((Sarg) rexVal).rangeSet;
            Set<Range<Comparable>> set = rangeSet.asRanges();
            fn = m -> {
                Object o = m[colIndex];
                if (o == null) return false;
                for (Range<Comparable> range : set) {
                    if (!range.hasUpperBound() && !range.hasLowerBound()) {
                        continue;
                    }
                    if (!range.hasUpperBound()) {
                        if (compare(o, range.lowerEndpoint()) >= 0) {
                            return true;
                        }
                    } else if (!range.hasLowerBound()) {
                        if (compare(o, range.upperEndpoint()) <= 0) {
                            return true;
                        }
                    } else {
                        Comparable u = range.upperEndpoint();
                        Comparable l = range.lowerEndpoint();
                        if (compare(o, u) <= 0 && compare(o, l) >= 0) {
                            // 数据在l和u之间，符合要求
                            return true;
                        }
                    }
                }
                return false;
            };
        } else {
            LOG.warn("operation not support: " + condition.toString());
//            throw new UnsupportedOperationException("operation not support: " + condition.toString());
        }
        // TODO + -
        return fn;
    }

    private int compare(Object dataVal, Comparable rexVal) {
        if (dataVal == rexVal) {
            return 0;
        }
        if (dataVal == null) {
            return -1;
        }
        if (rexVal == null) {
            return 1;
        }
        if (rexVal.getClass() == dataVal.getClass()) {
            return ((Comparable)dataVal).compareTo(rexVal);
        }
        if (rexVal instanceof BigDecimal) {
            return new BigDecimal(dataVal.toString()).compareTo((BigDecimal)rexVal);
        }
        if (rexVal instanceof NlsString) {
            return ((Comparable)dataVal).compareTo(((NlsString)rexVal).getValue());
        }
        if (rexVal instanceof TimestampString) {
            long l_rexVal = ((TimestampString)rexVal).getMillisSinceEpoch();
            return compare(toLong(dataVal), l_rexVal);
        }
//        if (rexVal instanceof TimestampWithTimeZoneString) {
//            long l_rexVal = ((TimestampWithTimeZoneString)rexVal).getLocalTimestampString().getMillisSinceEpoch();
//            return compare(toLong(dataVal), l_rexVal);
//        }
//        if (rexVal instanceof TimeWithTimeZoneString) {
//            int i_rexVal = ((TimeWithTimeZoneString)rexVal).getLocalTimeString().getMillisOfDay();
//            return compare(toLong(dataVal), i_rexVal);
//        }
        if (rexVal instanceof TimeString) {
            int i_rexVal = ((TimeString)rexVal).getMillisOfDay();
            return compare(toLong(dataVal), i_rexVal);
        }
        if (rexVal instanceof DateString) {
            long l_rexVal = ((DateString)rexVal).getMillisSinceEpoch();
            return compare(toLong(dataVal), l_rexVal);
        }
        if (rexVal instanceof Calendar) {
            long l_rexVal = ((Calendar)rexVal).getTime().getTime();
            return compare(toLong(dataVal), l_rexVal);
        }
        return ((Comparable)dataVal).compareTo(rexVal);
    }

    private long toLong(Object dataVal) {
        if (dataVal instanceof Date) {
            return ((Date)dataVal).getTime();
        } else if (dataVal instanceof Number) {
            return ((Number)dataVal).longValue();
        } else {
            return Long.valueOf(dataVal.toString());
        }
    }

    private int compare(long l1, long l2) {
        if (l1 == l2) {
            return 0;
        }
        if (l1 > l2) {
            return 1;
        }
        return -1;
    }

    private Comparable parseValue(RexNode rexNode/*, int colIndex*/) {
//        DataType dt = columnArray[colIndex].getDataType();
        Comparable val = null;
        if (rexNode instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral)rexNode;
//            if (literal.getTypeName() == SqlTypeName.SARG) {
            val = literal.getValue();
//            } else {
//                val = literal.getValueAs(dt.getClazz());
//            }
        }
//        else if (rexNode instanceof SqlNodeList) {
//            val = ((SqlNodeList) sqlNode).getList().stream().map(s -> {
//                if (s instanceof SqlLiteral) {
//                    return dt.parseValue(((SqlLiteral) s).toValue());
//                }
//                throw new UnsupportedOperationException("operation not support: " + s.toString());
//            }).collect(Collectors.toList());
//        }
        else {
            LOG.warn("operation not support: " + rexNode.toString());
        }
        return val;
    }

    private void closeIterator(Iterator<Object[]> iterator) {
        if (iterator instanceof AutoCloseable) {
            try {
                ((AutoCloseable) iterator).close();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
