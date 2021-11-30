package com.yuanzhy.sqldog.server.sql.function.agg;


/**
 *
 * @author yuanzhy
 * @date 2021-11-29
 */
public interface AggFunction<T> {

    T init();

    String result(T fn);

    //T add(T fn, Object val, xxx);
}
